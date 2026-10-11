package workspacestats

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/xerrors"

	"cdr.dev/slog/v3"
	"cdr.dev/slog/v3/sloggers/sloghuman"
	agentproto "github.com/coder/coder/v2/agent/proto"
	"github.com/coder/coder/v2/coderd/database"
	"github.com/coder/coder/v2/coderd/database/dbauthz"
	"github.com/coder/coder/v2/coderd/database/dbtime"
)

const (
	defaultBufferSize    = 1024
	defaultFlushInterval = time.Second

	// shutdownFlushTimeout bounds the best-effort drain of in-memory reports
	// when the process stops. Batches that cannot make it to the database in
	// that window are spooled for the next start.
	shutdownFlushTimeout = 15 * time.Second
	// minRetryBackoff/maxRetryBackoff pace retries of complete batches while
	// the database is unavailable.
	minRetryBackoff = 200 * time.Millisecond
	maxRetryBackoff = 30 * time.Second

	spoolSuffix = ".json"
	tmpSuffix   = ".tmp"
	badSuffix   = ".corrupt"
)

type Batcher interface {
	Add(now time.Time, agentID uuid.UUID, templateID uuid.UUID, userID uuid.UUID, workspaceID uuid.UUID, st *agentproto.Stats, usage bool)
}

// pendingBatch is a fully prepared, self-contained insert. Its JSON fields are
// already marshaled, so the batch can be retried, spooled to disk and recovered
// without touching or blocking the active accumulation buffer.
type pendingBatch struct {
	params    database.InsertWorkspaceAgentStatsParams
	spoolPath string
	attempts  int
}

// DBBatcher holds a buffer of agent stats and periodically flushes them to
// its configured store.
//
// Accepted reports are never lost to a database outage:
//
//   - Complete batches that fail to insert stay intact and are retried with
//     backoff. New reports keep accumulating in a fresh buffer, so one failed
//     batch never blocks later reports.
//   - Failed batches are spooled to disk (when a spool directory is
//     configured), and spooled batches are recovered on the next start.
//   - Row IDs are generated on acceptance and carried through every replay,
//     so inserting (and the template usage rollup that follows) stays
//     idempotent thanks to ON CONFLICT DO NOTHING.
type DBBatcher struct {
	store    database.Store
	log      slog.Logger
	spoolDir string

	mu sync.Mutex
	// TODO: make this a buffered chan instead?
	buf *database.InsertWorkspaceAgentStatsParams
	// These objects are marshaled into positional arrays on flush.
	connectionsByProto []map[string]int64
	sessionCounts      []map[string]int64
	batchSize          int
	// queue holds complete batches waiting to be (re)inserted, oldest first.
	// Batches recovered from the spool directory and batches whose insert
	// failed live here.
	queue []*pendingBatch

	// tickCh is used to periodically flush the buffer.
	tickCh   <-chan time.Time
	ticker   *time.Ticker
	interval time.Duration
	// flushLever is used to signal the flusher to flush the buffer immediately.
	flushLever  chan struct{}
	flushForced atomic.Bool
	// retryTimer fires when queued batches should be retried after a failure.
	// It is nil while there is nothing to retry.
	retryTimer *time.Timer
	// retryMinBackoff/retryMaxBackoff default to the package constants and can
	// be overridden in tests to keep flush timing deterministic.
	retryMinBackoff time.Duration
	retryMaxBackoff time.Duration
	// flushed is used during testing to signal that a flush has completed.
	flushed chan<- int

	metrics batcherMetrics
	// registerer is nil unless BatcherWithRegisterer is passed.
	registerer prometheus.Registerer
}

// Option is a functional option for configuring a Batcher.
type BatcherOption func(b *DBBatcher)

// BatcherWithStore sets the store to use for storing stats.
func BatcherWithStore(store database.Store) BatcherOption {
	return func(b *DBBatcher) {
		b.store = store
	}
}

// BatcherWithBatchSize sets the number of stats to store in a batch.
func BatcherWithBatchSize(size int) BatcherOption {
	return func(b *DBBatcher) {
		b.batchSize = size
	}
}

// BatcherWithInterval sets the interval for flushes.
func BatcherWithInterval(d time.Duration) BatcherOption {
	return func(b *DBBatcher) {
		b.interval = d
	}
}

// BatcherWithLogger sets the logger to use for logging.
func BatcherWithLogger(log slog.Logger) BatcherOption {
	return func(b *DBBatcher) {
		b.log = log
	}
}

// BatcherWithRegisterer sets the Prometheus registerer for batcher metrics.
func BatcherWithRegisterer(reg prometheus.Registerer) BatcherOption {
	return func(b *DBBatcher) {
		b.registerer = reg
	}
}

// BatcherWithSpoolDir enables durable recovery. Batches that cannot be
// inserted because the database is unavailable are written as complete files
// into dir, and batches found there are replayed (idempotently) when the
// batcher starts. An empty dir disables spooling: recovery is then limited to
// the lifetime of the process.
func BatcherWithSpoolDir(dir string) BatcherOption {
	return func(b *DBBatcher) {
		b.spoolDir = dir
	}
}

// NewBatcher creates a new Batcher and starts it.
func NewBatcher(ctx context.Context, opts ...BatcherOption) (*DBBatcher, func(), error) {
	b := &DBBatcher{}
	b.log = slog.Make(sloghuman.Sink(os.Stderr))
	b.metrics = newBatcherMetrics()
	b.flushLever = make(chan struct{}, 1) // Buffered so that it doesn't block.
	for _, opt := range opts {
		opt(b)
	}

	b.metrics.register(b.registerer)

	if b.store == nil {
		return nil, nil, xerrors.Errorf("no store configured for batcher")
	}

	if b.interval == 0 {
		b.interval = defaultFlushInterval
	}

	if b.batchSize == 0 {
		b.batchSize = defaultBufferSize
	}

	if b.tickCh == nil {
		b.ticker = time.NewTicker(b.interval)
		b.tickCh = b.ticker.C
	}

	b.initBuf(b.batchSize)

	if b.spoolDir != "" {
		if err := os.MkdirAll(b.spoolDir, 0o700); err != nil {
			return nil, nil, xerrors.Errorf("create agent stats spool directory: %w", err)
		}
		b.recoverSpool()
	}

	// Recovered batches belong to reports accepted before this process
	// started, so replay them immediately rather than waiting for a tick.
	if len(b.queue) > 0 {
		select {
		case b.flushLever <- struct{}{}:
		default:
		}
	}

	cancelCtx, cancelFunc := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		b.run(cancelCtx)
		close(done)
	}()

	closer := func() {
		cancelFunc()
		if b.ticker != nil {
			b.ticker.Stop()
		}
		<-done
	}

	return b, closer, nil
}

// Add adds a stat to the batcher for the given workspace and agent.
// It never blocks on database I/O: reports keep landing in the active buffer
// even while earlier batches are queued for retry.
func (b *DBBatcher) Add(
	now time.Time,
	agentID uuid.UUID,
	templateID uuid.UUID,
	userID uuid.UUID,
	workspaceID uuid.UUID,
	st *agentproto.Stats,
	usage bool,
) {
	// Normalize and cap outside the lock.
	sessionCounts, overflow := capSessionCounts(normalizedSessionCounts(st))
	if overflow > 0 {
		// A misbehaving agent hits this on every report, so alert on the
		// counter and keep the log at debug.
		b.log.Debug(context.Background(), "too many distinct session types, excess counted under overflow",
			slog.F("agent_id", agentID),
			slog.F("overflow", overflow),
			slog.F("max", maxSessionCountEntries),
		)
		b.metrics.SessionCountsOverflowTotal.Add(float64(overflow))
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now = dbtime.Time(now)

	b.buf.ID = append(b.buf.ID, uuid.New())
	b.buf.CreatedAt = append(b.buf.CreatedAt, now)
	b.buf.AgentID = append(b.buf.AgentID, agentID)
	b.buf.UserID = append(b.buf.UserID, userID)
	b.buf.TemplateID = append(b.buf.TemplateID, templateID)
	b.buf.WorkspaceID = append(b.buf.WorkspaceID, workspaceID)

	b.connectionsByProto = append(b.connectionsByProto, st.ConnectionsByProto)
	b.sessionCounts = append(b.sessionCounts, sessionCounts)

	b.buf.ConnectionCount = append(b.buf.ConnectionCount, st.ConnectionCount)
	b.buf.RxPackets = append(b.buf.RxPackets, st.RxPackets)
	b.buf.RxBytes = append(b.buf.RxBytes, st.RxBytes)
	b.buf.TxPackets = append(b.buf.TxPackets, st.TxPackets)
	b.buf.TxBytes = append(b.buf.TxBytes, st.TxBytes)
	b.buf.ConnectionMedianLatencyMS = append(b.buf.ConnectionMedianLatencyMS, st.ConnectionMedianLatencyMs)
	b.buf.Usage = append(b.buf.Usage, usage)

	// If the buffer is over 80% full, signal the flusher to flush immediately.
	// We want to trigger flushes early to reduce the likelihood of
	// accidentally growing the buffer over batchSize.
	filled := float64(len(b.buf.ID)) / float64(b.batchSize)
	if filled >= 0.8 && !b.flushForced.Load() {
		b.flushLever <- struct{}{}
		b.flushForced.Store(true)
	}
}

// Run runs the batcher.
func (b *DBBatcher) run(ctx context.Context) {
	// nolint:gocritic // This is only ever used for one thing - inserting agent stats.
	authCtx := dbauthz.AsSystemRestricted(ctx)
	// A nil retry channel blocks forever; it is replaced once a batch fails.
	var retry <-chan time.Time
	for {
		select {
		case <-b.tickCh:
			retry = b.flush(authCtx, "scheduled")
		case <-b.flushLever:
			// If the flush lever is depressed, flush the buffer immediately.
			retry = b.flush(authCtx, "reaching capacity")
		case <-retry:
			retry = b.flush(authCtx, "retry")
		case <-ctx.Done():
			b.log.Debug(ctx, "context done, flushing before exit")

			// We must create a new context here as the parent context is done.
			ctxTimeout, cancel := context.WithTimeout(context.Background(), shutdownFlushTimeout)

			// nolint:gocritic // This is only ever used for one thing - inserting agent stats.
			b.drain(dbauthz.AsSystemRestricted(ctxTimeout))
			cancel()
			return
		}
	}
}

// flush inserts every currently queued or buffered batch. Batches older than
// the active buffer are inserted first. The returned channel fires when
// failed batches should be retried; it is nil when all inserts succeeded.
func (b *DBBatcher) flush(ctx context.Context, reason string) (nextRetry <-chan time.Time) {
	start := time.Now()
	b.mu.Lock()
	b.flushForced.Store(true)
	batches := b.takeBatchesLocked()
	b.mu.Unlock()

	count := 0
	for _, batch := range batches {
		count += len(batch.params.ID)
	}
	defer func() {
		b.mu.Lock()
		b.flushForced.Store(false)
		b.mu.Unlock()
		if count > 0 {
			elapsed := time.Since(start)
			b.log.Debug(ctx, "flush complete",
				slog.F("count", count),
				slog.F("elapsed", elapsed),
				slog.F("reason", reason),
			)
		}
		// Notify that a flush has completed. This only happens in tests.
		b.notifyFlush(ctx, count)
	}()

	if len(batches) == 0 {
		b.stopRetryTimer()
		return nil
	}

	failed := b.insertBatches(ctx, batches)
	if len(failed) == 0 {
		b.stopRetryTimer()
		return nil
	}

	// Keep the failed batches ahead of anything that accumulates later.
	b.mu.Lock()
	b.queue = append(failed, b.queue...)
	attempts := 0
	for _, batch := range failed {
		if batch.attempts > attempts {
			attempts = batch.attempts
		}
	}
	b.mu.Unlock()

	b.log.Error(ctx, "failed to insert workspace agent stats, batches retained for retry",
		slog.F("failed_batches", len(failed)),
		slog.F("rows", count),
		slog.F("attempt", attempts),
		slog.F("elapsed", time.Since(start)),
	)
	return b.resetRetryTimer(b.backoff(attempts))
}

// drain attempts to insert everything still in memory at shutdown. Batches
// that cannot be inserted within the context are spooled for the next start,
// so a database outage during stop does not drop accepted reports.
func (b *DBBatcher) drain(ctx context.Context) {
	b.mu.Lock()
	b.flushForced.Store(true)
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.flushForced.Store(false)
		b.mu.Unlock()
	}()

	for {
		if err := ctx.Err(); err != nil {
			b.spoolRemaining(ctx)
			return
		}
		b.mu.Lock()
		batches := b.takeBatchesLocked()
		b.mu.Unlock()
		if len(batches) == 0 {
			return
		}
		if failed := b.insertBatches(ctx, batches); len(failed) > 0 {
			b.mu.Lock()
			b.queue = append(failed, b.queue...)
			b.mu.Unlock()
			// The database is unavailable and the shutdown window is finite:
			// get every batch onto disk instead of retrying.
			b.spoolRemaining(ctx)
			return
		}
	}
}

// takeBatchesLocked detaches all insertable batches from the batcher: queued
// (older) batches first, followed by the prepared active buffer. b MUST be
// locked. The returned batches are owned by the caller.
func (b *DBBatcher) takeBatchesLocked() []*pendingBatch {
	batches := b.queue
	b.queue = nil
	if len(b.buf.ID) > 0 {
		batches = append(batches, b.prepareBatchLocked())
	}
	return batches
}

// prepareBatchLocked marshals the active buffer into a standalone batch and
// replaces the active buffer with a fresh one, so new reports can accumulate
// while the batch is in flight. b MUST be locked.
func (b *DBBatcher) prepareBatchLocked() *pendingBatch {
	payload, err := json.Marshal(b.connectionsByProto)
	if err != nil {
		b.log.Error(context.Background(), "unable to marshal agent connections by proto, dropping data", slog.Error(err))
		b.buf.ConnectionsByProto = json.RawMessage(`[]`)
	} else {
		b.buf.ConnectionsByProto = payload
	}

	sessionCountsPayload, err := json.Marshal(b.sessionCounts)
	if err != nil {
		b.log.Error(context.Background(), "unable to marshal agent session counts, dropping data", slog.Error(err))
		b.buf.SessionCounts = json.RawMessage(`[]`)
	} else {
		b.buf.SessionCounts = sessionCountsPayload
	}

	batch := &pendingBatch{params: *b.buf}
	b.initBuf(b.batchSize)
	return batch
}

// insertBatches inserts batches in order and returns the ones that did not
// make it, oldest first. A failure never stops the loop, so one bad or
// temporarily failing batch cannot hold up the rest. Every returned batch has
// been spooled when spooling is enabled.
func (b *DBBatcher) insertBatches(ctx context.Context, batches []*pendingBatch) []*pendingBatch {
	var failed []*pendingBatch
	for i, batch := range batches {
		if err := ctx.Err(); err != nil {
			// No time left to attempt the rest; the caller spools them.
			failed = append(failed, batches[i:]...)
			break
		}
		batch.attempts++
		err := b.store.InsertWorkspaceAgentStats(ctx, batch.params)
		if err == nil {
			b.spoolRemove(batch)
			continue
		}
		if database.IsQueryCanceledError(err) {
			b.log.Debug(ctx, "query canceled inserting workspace agent stats, batch retained",
				slog.F("rows", len(batch.params.ID)),
			)
		} else {
			b.log.Error(ctx, "error inserting workspace agent stats, batch retained",
				slog.Error(err),
				slog.F("rows", len(batch.params.ID)),
				slog.F("attempt", batch.attempts),
			)
		}
		b.spoolPersist(ctx, batch)
		failed = append(failed, batch)
	}
	return failed
}

func (b *DBBatcher) notifyFlush(ctx context.Context, count int) {
	if b.flushed == nil {
		return
	}
	select {
	case <-ctx.Done():
		close(b.flushed)
	default:
		b.flushed <- count
	}
}

func (b *DBBatcher) resetRetryTimer(d time.Duration) <-chan time.Time {
	b.stopRetryTimer()
	b.retryTimer = time.NewTimer(d)
	return b.retryTimer.C
}

func (b *DBBatcher) stopRetryTimer() {
	if b.retryTimer != nil {
		b.retryTimer.Stop()
		b.retryTimer = nil
	}
}

func (b *DBBatcher) backoff(attempts int) time.Duration {
	minD := minRetryBackoff
	if b.retryMinBackoff > 0 {
		minD = b.retryMinBackoff
	}
	maxD := maxRetryBackoff
	if b.retryMaxBackoff > 0 {
		maxD = b.retryMaxBackoff
	}
	if attempts < 1 {
		attempts = 1
	}
	shift := attempts - 1
	if shift > 8 {
		shift = 8
	}
	d := minD << shift
	if d <= 0 || d > maxD {
		return maxD
	}
	return d
}

// initBuf resets the buffer. b MUST be locked.
func (b *DBBatcher) initBuf(size int) {
	b.buf = &database.InsertWorkspaceAgentStatsParams{
		ID:                        make([]uuid.UUID, 0, b.batchSize),
		CreatedAt:                 make([]time.Time, 0, b.batchSize),
		UserID:                    make([]uuid.UUID, 0, b.batchSize),
		WorkspaceID:               make([]uuid.UUID, 0, b.batchSize),
		TemplateID:                make([]uuid.UUID, 0, b.batchSize),
		AgentID:                   make([]uuid.UUID, 0, b.batchSize),
		ConnectionsByProto:        json.RawMessage("[]"),
		ConnectionCount:           make([]int64, 0, b.batchSize),
		RxPackets:                 make([]int64, 0, b.batchSize),
		RxBytes:                   make([]int64, 0, b.batchSize),
		TxPackets:                 make([]int64, 0, b.batchSize),
		TxBytes:                   make([]int64, 0, b.batchSize),
		SessionCounts:             json.RawMessage("[]"),
		ConnectionMedianLatencyMS: make([]float64, 0, b.batchSize),
		Usage:                     make([]bool, 0, b.batchSize),
	}

	b.connectionsByProto = make([]map[string]int64, 0, size)
	b.sessionCounts = make([]map[string]int64, 0, size)
}

// recoverSpool loads batches previously persisted to the spool directory into
// the retry queue, oldest first. Unreadable or malformed files are quarantined
// instead of blocking startup or later reports.
func (b *DBBatcher) recoverSpool() {
	ctx := context.Background()
	entries, err := os.ReadDir(b.spoolDir)
	if err != nil {
		if !os.IsNotExist(err) {
			b.log.Warn(ctx, "unable to read agent stats spool directory, spooled batches skipped",
				slog.F("dir", b.spoolDir), slog.Error(err))
		}
		return
	}

	recoveredBatches := 0
	recoveredRows := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		switch {
		case strings.HasSuffix(name, tmpSuffix):
			// Leftover from a process that died mid-write; the final file was
			// never renamed into place.
			if err := os.Remove(filepath.Join(b.spoolDir, name)); err != nil {
				b.log.Warn(ctx, "unable to remove stale agent stats spool temp file",
					slog.F("path", name), slog.Error(err))
			}
		case strings.HasSuffix(name, spoolSuffix):
			batch, err := b.readSpoolFile(name)
			if err != nil {
				b.quarantineSpool(name, err)
				continue
			}
			b.queue = append(b.queue, batch)
			recoveredBatches++
			recoveredRows += len(batch.params.ID)
		default:
			// Quarantined files (ending in badSuffix) and unrelated files are
			// left untouched.
		}
	}
	if recoveredBatches > 0 {
		b.log.Info(ctx, "recovered spooled agent stats batches",
			slog.F("batches", recoveredBatches),
			slog.F("rows", recoveredRows),
		)
	}
}

func (b *DBBatcher) readSpoolFile(name string) (*pendingBatch, error) {
	data, err := os.ReadFile(filepath.Join(b.spoolDir, name))
	if err != nil {
		return nil, xerrors.Errorf("read spooled agent stats batch: %w", err)
	}
	var params database.InsertWorkspaceAgentStatsParams
	if err := json.Unmarshal(data, &params); err != nil {
		return nil, xerrors.Errorf("decode spooled agent stats batch: %w", err)
	}
	if err := validateAgentStatsParams(&params); err != nil {
		return nil, xerrors.Errorf("validate spooled agent stats batch: %w", err)
	}
	return &pendingBatch{params: params, spoolPath: filepath.Join(b.spoolDir, name)}, nil
}

// validateAgentStatsParams asserts the positional arrays survived spooling
// intact: all columns must carry exactly one entry per row.
func validateAgentStatsParams(p *database.InsertWorkspaceAgentStatsParams) error {
	n := len(p.ID)
	if n == 0 {
		return xerrors.New("batch contains no rows")
	}
	checks := []struct {
		name string
		got  int
	}{
		{"created_at", len(p.CreatedAt)},
		{"user_id", len(p.UserID)},
		{"workspace_id", len(p.WorkspaceID)},
		{"template_id", len(p.TemplateID)},
		{"agent_id", len(p.AgentID)},
		{"connection_count", len(p.ConnectionCount)},
		{"rx_packets", len(p.RxPackets)},
		{"rx_bytes", len(p.RxBytes)},
		{"tx_packets", len(p.TxPackets)},
		{"tx_bytes", len(p.TxBytes)},
		{"connection_median_latency_ms", len(p.ConnectionMedianLatencyMS)},
		{"usage", len(p.Usage)},
	}
	for _, check := range checks {
		if check.got != n {
			return xerrors.Errorf("column %s has %d entries, want %d", check.name, check.got, n)
		}
	}
	var connections []json.RawMessage
	if err := json.Unmarshal(p.ConnectionsByProto, &connections); err != nil {
		return xerrors.Errorf("decode connections_by_proto: %w", err)
	}
	if len(connections) != n {
		return xerrors.Errorf("column connections_by_proto has %d entries, want %d", len(connections), n)
	}
	var sessionCounts []json.RawMessage
	if err := json.Unmarshal(p.SessionCounts, &sessionCounts); err != nil {
		return xerrors.Errorf("decode session_counts: %w", err)
	}
	if len(sessionCounts) != n {
		return xerrors.Errorf("column session_counts has %d entries, want %d", len(sessionCounts), n)
	}
	return nil
}

func (b *DBBatcher) quarantineSpool(name string, cause error) {
	bad := filepath.Join(b.spoolDir, name+badSuffix)
	if err := os.Rename(filepath.Join(b.spoolDir, name), bad); err != nil {
		b.log.Error(context.Background(), "unable to quarantine corrupt agent stats spool batch",
			slog.F("path", name), slog.Error(err))
		return
	}
	b.log.Error(context.Background(), "quarantined corrupt agent stats spool batch; other batches and reports are unaffected",
		slog.F("path", bad), slog.Error(cause))
}

// spoolPersist durably writes a batch that failed to insert. It is a no-op
// when spooling is disabled or the batch is already on disk.
func (b *DBBatcher) spoolPersist(ctx context.Context, batch *pendingBatch) {
	if b.spoolDir == "" {
		return
	}
	b.mu.Lock()
	if batch.spoolPath != "" {
		b.mu.Unlock()
		return
	}
	dir := b.spoolDir
	b.mu.Unlock()

	data, err := json.Marshal(batch.params)
	if err != nil {
		b.log.Error(ctx, "unable to encode agent stats batch for spool, batch retained in memory only", slog.Error(err))
		return
	}
	path, err := writeSpoolFile(dir, data)
	if err != nil {
		b.log.Error(ctx, "unable to spool agent stats batch, batch retained in memory only", slog.Error(err))
		return
	}
	b.mu.Lock()
	if batch.spoolPath == "" {
		batch.spoolPath = path
	} else {
		// The batch was spooled concurrently; drop the duplicate file.
		_ = os.Remove(path)
	}
	b.mu.Unlock()
}

// spoolRemaining writes every queued or buffered batch to disk without an
// insert attempt. Used during shutdown when the database cannot be reached
// before the drain deadline.
func (b *DBBatcher) spoolRemaining(ctx context.Context) {
	if b.spoolDir == "" {
		b.log.Warn(ctx, "agent stats may be lost: database is unavailable and no spool directory is configured")
		return
	}
	b.mu.Lock()
	batches := b.queue
	b.queue = nil
	if len(b.buf.ID) > 0 {
		batches = append(batches, b.prepareBatchLocked())
	}
	b.mu.Unlock()

	for _, batch := range batches {
		b.spoolPersist(ctx, batch)
	}
}

func (b *DBBatcher) spoolRemove(batch *pendingBatch) {
	b.mu.Lock()
	path := batch.spoolPath
	batch.spoolPath = ""
	b.mu.Unlock()
	if path == "" {
		return
	}
	// A file that cannot be removed is replayed on the next start; ON CONFLICT
	// keeps the replay from double-counting.
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		b.log.Warn(context.Background(), "failed to remove spooled agent stats batch; it will be replayed once",
			slog.F("path", path), slog.Error(err))
	}
}

// writeSpoolFile writes data atomically: a temp file in dir is renamed into
// place, so recovery never sees a half-written batch. Filenames sort by the
// time they were spooled, which is also the order batches are replayed.
func writeSpoolFile(dir string, data []byte) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", xerrors.Errorf("create spool directory: %w", err)
	}
	name := fmt.Sprintf("%019d-%s%s", time.Now().UnixNano(), uuid.NewString(), spoolSuffix)
	tmpPath := filepath.Join(dir, "."+name+tmpSuffix)
	finalPath := filepath.Join(dir, name)
	//nolint:gosec // dir is operator configured and the file name is fully generated here.
	if err := os.WriteFile(tmpPath, data, 0o600); err != nil {
		return "", xerrors.Errorf("write spool file: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", xerrors.Errorf("finalize spool file: %w", err)
	}
	return finalPath, nil
}
