package notifications

import (
	"context"
	"sync"
	"text/template"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	"cdr.dev/slog/v3"
	"github.com/coder/coder/v2/coderd/database"
	"github.com/coder/coder/v2/coderd/database/pubsub"
	"github.com/coder/coder/v2/coderd/notifications/dispatch"
	"github.com/coder/coder/v2/coderd/pproflabel"
	"github.com/coder/coder/v2/codersdk"
	"github.com/coder/quartz"
)

var ErrInvalidDispatchTimeout = xerrors.New("dispatch timeout must be less than lease period")

// storeCallTimeout bounds each bulk update of dispatch results to the store.
const storeCallTimeout = 30 * time.Second

// shutdownFlushAttempts bounds how many times the manager attempts to persist buffered results
// while shutting down. Results still unwritten after the final attempt remain recoverable: their
// leases expire and the messages are re-acquired and re-dispatched.
const shutdownFlushAttempts = 3

// shutdownFlushBackoff is the wait between shutdown flush attempts.
const shutdownFlushBackoff = 200 * time.Millisecond

// Manager manages all notifications being enqueued and dispatched.
//
// Manager maintains a notifier: this consumes the queue of notification messages in the store.
//
// The notifier dequeues messages from the store _CODER_NOTIFICATIONS_LEASE_COUNT_ at a time and concurrently "dispatches"
// these messages, meaning they are sent by their respective methods (email, webhook, etc).
//
// To reduce load on the store, successful and failed dispatches are first accumulated in two
// separate buffered channels (success/failure) of size CODER_NOTIFICATIONS_STORE_SYNC_INTERVAL,
// then drained into a pending backlog which is bulk-written to the store every
// CODER_NOTIFICATIONS_STORE_SYNC_INTERVAL seconds. A result leaves the backlog only once its
// store write is acknowledged: a failed bulk update retains its results and retries them on the
// next flush, so a transient database error or a canceled context cannot drop an already produced
// result. Graceful Stop() drains the backlog before returning; an ungraceful context cancellation
// makes a bounded best-effort flush before exiting, and anything beyond that is recovered when
// the message lease expires (at-least-once delivery).
//
// The bulk updates only apply while the producing notifier still owns the message lease, so stale
// results left over from an expired lease cannot overwrite a sent, temporary_failure,
// permanent_failure or inhibited state already confirmed by a later dispatch.
//
// These buffers naturally introduce some backpressure; if there are hundreds of messages to be
// sent but they start failing too quickly, the buffers (receive channels) will fill up and block
// senders, which will slow down the dispatch rate.
//
// NOTE: The above backpressure mechanism only works within the same process, which may not be true forever, such as if
// we split notifiers out into separate targets for greater processing throughput; in this case we will need an
// alternative mechanism for handling backpressure.
type Manager struct {
	cfg codersdk.NotificationsConfig

	store Store
	log   slog.Logger

	handlers map[database.NotificationMethod]Handler
	method   database.NotificationMethod
	helpers  template.FuncMap

	metrics *Metrics

	// success/failure are ingestion channels written to by the notifier and drained by the store
	// sync goroutine into the pending backlog.
	success, failure chan dispatchResult

	// resultsMu guards pendingSuccess/pendingFailure. The store sync goroutine is their sole
	// mutator; BufferedUpdatesCount reads them concurrently.
	resultsMu      sync.Mutex
	pendingSuccess []dispatchResult
	pendingFailure []dispatchResult

	// flushMu ensures bulk updates do not overlap. A tick fired while a flush is still waiting
	// on a slow store is skipped rather than piling duplicate bulk updates onto the store; the
	// in-flight flush owns the retained backlog. Shutdown flushes take the lock blocking.
	flushMu sync.Mutex

	mu       sync.Mutex // Protects following.
	closed   bool
	notifier *notifier

	runOnce sync.Once
	stop    chan any
	done    chan any

	// clock is for testing only
	clock quartz.Clock
}

type ManagerOption func(*Manager)

// WithTestClock is used in testing to set the quartz clock on the manager
func WithTestClock(clock quartz.Clock) ManagerOption {
	return func(m *Manager) {
		m.clock = clock
	}
}

// NewManager instantiates a new Manager instance which coordinates notification enqueuing and delivery.
//
// helpers is a map of template helpers which are used to customize notification messages to use global settings like
// access URL etc.
func NewManager(cfg codersdk.NotificationsConfig, store Store, ps pubsub.Pubsub, helpers template.FuncMap, metrics *Metrics, log slog.Logger, opts ...ManagerOption) (*Manager, error) {
	var method database.NotificationMethod
	if err := method.Scan(cfg.Method.String()); err != nil {
		return nil, xerrors.Errorf("notification method %q is invalid", cfg.Method)
	}

	// If dispatch timeout exceeds lease period, it is possible that messages can be delivered in duplicate because the
	// lease can expire before the notifier gives up on the dispatch, which results in the message becoming eligible for
	// being re-acquired.
	if cfg.DispatchTimeout.Value() >= cfg.LeasePeriod.Value() {
		return nil, ErrInvalidDispatchTimeout
	}

	m := &Manager{
		log:   log,
		cfg:   cfg,
		store: store,

		// Buffer successful/failed notification dispatches in memory to reduce load on the store.
		//
		// We keep separate buffered for success/failure right now because the bulk updates are already a bit janky,
		// see BulkMarkNotificationMessagesSent/BulkMarkNotificationMessagesFailed. If we had the ability to batch updates,
		// like is offered in https://docs.sqlc.dev/en/stable/reference/query-annotations.html#batchmany, we'd have a cleaner
		// approach to this - but for now this will work fine.
		success: make(chan dispatchResult, cfg.StoreSyncBufferSize),
		failure: make(chan dispatchResult, cfg.StoreSyncBufferSize),

		metrics: metrics,
		method:  method,

		stop: make(chan any),
		done: make(chan any),

		handlers: defaultHandlers(cfg, log, store, ps),
		helpers:  helpers,

		clock: quartz.NewReal(),
	}
	for _, o := range opts {
		o(m)
	}
	return m, nil
}

// defaultHandlers builds a set of known handlers; panics if any error occurs as these handlers should be valid at compile time.
func defaultHandlers(cfg codersdk.NotificationsConfig, log slog.Logger, store Store, ps pubsub.Pubsub) map[database.NotificationMethod]Handler {
	return map[database.NotificationMethod]Handler{
		database.NotificationMethodSmtp:    dispatch.NewSMTPHandler(cfg.SMTP, log.Named("dispatcher.smtp")),
		database.NotificationMethodWebhook: dispatch.NewWebhookHandler(cfg.Webhook, log.Named("dispatcher.webhook")),
		database.NotificationMethodInbox:   dispatch.NewInboxHandler(log.Named("dispatcher.inbox"), store, ps),
	}
}

// WithHandlers allows for tests to inject their own handlers to verify functionality.
func (m *Manager) WithHandlers(reg map[database.NotificationMethod]Handler) {
	m.handlers = reg
}

var ErrManagerAlreadyClosed = xerrors.New("manager already closed")

// Run initiates the control loop in the background, which spawns a given number of notifier goroutines.
// Manager requires system-level permissions to interact with the store.
// Run is only intended to be run once.
func (m *Manager) Run(ctx context.Context) {
	m.log.Debug(ctx, "notification manager started")

	m.runOnce.Do(func() {
		// Closes when Stop() is called or context is canceled.
		pproflabel.Go(ctx, pproflabel.Service(pproflabel.ServiceNotifications), func(ctx context.Context) {
			err := m.loop(ctx)
			if err != nil {
				if xerrors.Is(err, ErrManagerAlreadyClosed) {
					m.log.Warn(ctx, "notification manager stopped with error", slog.Error(err))
				} else {
					m.log.Error(ctx, "notification manager stopped with error", slog.Error(err))
				}
			}
		})
	})
}

// loop contains the main business logic of the notification manager. It is responsible for subscribing to notification
// events, creating a notifier, and publishing bulk dispatch result updates to the store.
func (m *Manager) loop(ctx context.Context) error {
	defer func() {
		close(m.done)
		m.log.Debug(context.Background(), "notification manager stopped")
	}()

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrManagerAlreadyClosed
	}

	var eg errgroup.Group

	m.notifier = newNotifier(ctx, m.cfg, uuid.New(), m.log, m.store, m.handlers, m.helpers, m.metrics, m.clock)
	eg.Go(func() error {
		// run the notifier which will handle dequeueing and dispatching notifications.
		return m.notifier.run(m.success, m.failure)
	})

	// Periodically flush notification state changes to the store, and perform the final flush
	// when the manager stops.
	eg.Go(func() error {
		return m.runStoreSync(ctx)
	})

	m.mu.Unlock()

	err := eg.Wait()
	if err != nil {
		m.log.Error(ctx, "manager loop exited with error", slog.Error(err))
	}
	return err
}

// runStoreSync is the store synchronization loop. It periodically drains dispatch results into
// bulk store updates and performs the converging final flush when the manager stops or its context
// is canceled.
func (m *Manager) runStoreSync(ctx context.Context) error {
	// Every interval, collect the buffered messages and bulk update them in the store.
	tick := m.clock.NewTicker(m.cfg.StoreSyncInterval.Value(), "Manager", "storeSync")
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			// Ungraceful shutdown (e.g. the process context was canceled). Make a bounded
			// best-effort attempt to persist every result already produced, so dispatch outcomes
			// do not silently vanish. The flush context is detached from the canceled context so
			// the store calls keep their authorization and a bounded lifetime. Results that cannot
			// be persisted stay recoverable: their leases expire and the messages are re-acquired
			// and re-dispatched (at-least-once delivery).
			m.log.Warn(ctx, "notification context canceled; attempting best-effort flush of buffered results",
				slog.Error(ctx.Err()))
			m.shutdown(context.WithoutCancel(ctx), false)
			return ctx.Err()
		case <-m.stop:
			// Graceful Stop(): Stop() has already awaited the notifier, so all in-flight
			// dispatches completed and every result that will exist is buffered. Drain and flush
			// with a context that survives a manager context which was canceled concurrently.
			m.shutdown(context.WithoutCancel(ctx), true)
			return nil
		case <-tick.C:
			m.flushResults(ctx)
		}
	}
}

// shutdown awaits the dispatch producers and repeatedly flushes buffered results until they are
// all persisted or the attempt budget is exhausted. flushCtx is detached from the (possibly
// canceled) manager context by the caller, so store calls retain authorization but not
// cancellation.
func (m *Manager) shutdown(flushCtx context.Context, graceful bool) {
	// Move anything already buffered into the backlog first, freeing channel capacity for any
	// producer still blocked depositing a result under backpressure.
	m.drainPending()

	// Await the notifier so no result is produced after the final drain. On a graceful Stop() it
	// has already exited; on context cancellation the in-flight dispatch contexts are themselves
	// canceled, so this is bounded by the configured dispatch timeout. stop is idempotent, so a
	// race with an explicit Stop() is harmless.
	if m.notifier != nil {
		m.notifier.stop()
	}

	for attempt := 1; ; attempt++ {
		m.drainPending()
		if m.pendingTotal() == 0 {
			return
		}

		// Block behind any in-flight tick flush so the backlog is neither flushed twice nor
		// observed as pending while it is being cleared.
		m.flushMu.Lock()
		m.flushResultsLocked(flushCtx)
		m.flushMu.Unlock()

		m.drainPending()
		if m.pendingTotal() == 0 {
			return
		}
		if attempt >= shutdownFlushAttempts {
			break
		}

		m.log.Warn(flushCtx, "retrying flush of buffered notification results before shutdown",
			slog.F("attempt", attempt), slog.F("pending", m.pendingTotal()))
		timer := m.clock.NewTimer(shutdownFlushBackoff)
		<-timer.C
		timer.Stop()
	}

	success, failure := m.pendingCounts()
	m.log.Error(flushCtx, "giving up flushing buffered notification results during shutdown; "+
		"their leases will expire and the messages will be re-dispatched",
		slog.F("graceful", graceful),
		slog.F("success_count", success), slog.F("failure_count", failure))
}

// BufferedUpdatesCount returns the number of buffered updates which are currently waiting to be flushed to the store.
// The returned values are for success & failure, respectively. This includes results drained into the pending backlog
// that are awaiting a retry after a failed store update.
func (m *Manager) BufferedUpdatesCount() (success int, failure int) {
	m.resultsMu.Lock()
	defer m.resultsMu.Unlock()
	return len(m.success) + len(m.pendingSuccess), len(m.failure) + len(m.pendingFailure)
}

// flushResults drains newly produced dispatch results into the pending backlog and bulk-updates
// the store for all pending results. Ticks skipped while another flush is still in progress simply
// leave the backlog (and the ingestion channels) to the in-flight flush.
func (m *Manager) flushResults(ctx context.Context) {
	// A slow store can keep a flush running across several sync intervals. Skip those ticks
	// instead of queuing duplicate bulk updates, which would contend for the store and could
	// double-increment attempt counters if an ambiguous update eventually lands.
	if !m.flushMu.TryLock() {
		return
	}
	defer m.flushMu.Unlock()
	m.flushResultsLocked(ctx)
}

// flushResultsLocked bulk-updates the store for all pending results. The caller must hold
// flushMu.
//
// A result leaves the backlog only after the store acknowledges its write. When a bulk update
// fails (store unavailable, timeout, canceled context) the whole batch is retained and retried on
// the next flush. The success and failure batches are written independently, so a failure to
// persist one never blocks the other, and a single handler's results cannot block other
// handlers' results.
//
// The store only applies a result while the message is still leased by the notifier that
// produced it. Results whose lease expired and which were re-acquired by a (possibly different)
// notifier match no row once the statement succeeds: they are stale and are dropped. They can
// never overwrite a confirmed sent/temporary_failure/permanent_failure/inhibited state, and
// retrying an ambiguously timed-out update is safe (the retry is a no-op on already finalized
// rows, so attempt_count is not incremented twice).
func (m *Manager) flushResultsLocked(ctx context.Context) {
	m.drainPending()
	m.refreshPendingGauge()
	defer m.refreshPendingGauge()

	if m.pendingTotal() == 0 {
		return
	}

	var (
		successParams database.BulkMarkNotificationMessagesSentParams
		failureParams database.BulkMarkNotificationMessagesFailedParams
	)

	// Snapshot the backlog into query params; the pending slices remain the source of truth until
	// the store acknowledges the write.
	m.resultsMu.Lock()
	for _, res := range m.pendingSuccess {
		successParams.IDs = append(successParams.IDs, res.msg)
		successParams.SentAts = append(successParams.SentAts, res.ts)
		successParams.NotifierIDs = append(successParams.NotifierIDs, res.notifier)
	}
	for _, res := range m.pendingFailure {
		var (
			reason string
			status database.NotificationMessageStatus
		)

		switch {
		case res.retryable:
			status = database.NotificationMessageStatusTemporaryFailure
		case res.inhibited:
			status = database.NotificationMessageStatusInhibited
			reason = "disabled by user"
		default:
			status = database.NotificationMessageStatusPermanentFailure
		}
		if res.err != nil {
			reason = res.err.Error()
		}

		failureParams.IDs = append(failureParams.IDs, res.msg)
		failureParams.FailedAts = append(failureParams.FailedAts, res.ts)
		failureParams.NotifierIDs = append(failureParams.NotifierIDs, res.notifier)
		failureParams.Statuses = append(failureParams.Statuses, status)
		failureParams.StatusReasons = append(failureParams.StatusReasons, reason)
	}
	m.resultsMu.Unlock()

	// #nosec G115 - Safe conversion for max send attempts and retry interval, which are expected to be within int32 range.
	failureParams.MaxAttempts = int32(m.cfg.MaxSendAttempts)
	// #nosec G115 - Safe conversion for retry interval which is expected to be within int32 range.
	failureParams.RetryInterval = int32(m.cfg.RetryInterval.Value().Seconds())

	var (
		wg sync.WaitGroup

		sentOK bool
		sentN  int64

		failedOK bool
		failedN  int64
	)
	wg.Add(2)

	go func() {
		defer wg.Done()
		if len(successParams.IDs) == 0 {
			return
		}

		logger := m.log.With(slog.F("type", "update_sent"))

		// Bound how long we wait for the store.
		uctx, cancel := context.WithTimeout(ctx, storeCallTimeout)
		defer cancel()

		n, err := m.store.BulkMarkNotificationMessagesSent(uctx, successParams)
		if err != nil {
			logger.Error(ctx, "bulk update failed; results retained for a retry",
				slog.F("retained", len(successParams.IDs)), slog.Error(err))
			return
		}
		sentOK, sentN = true, n
		logger.Debug(ctx, "bulk update completed",
			slog.F("updated", n), slog.F("submitted", len(successParams.IDs)))
	}()

	go func() {
		defer wg.Done()
		if len(failureParams.IDs) == 0 {
			return
		}

		logger := m.log.With(slog.F("type", "update_failed"))

		// Bound how long we wait for the store.
		uctx, cancel := context.WithTimeout(ctx, storeCallTimeout)
		defer cancel()

		n, err := m.store.BulkMarkNotificationMessagesFailed(uctx, failureParams)
		if err != nil {
			logger.Error(ctx, "bulk update failed; results retained for a retry",
				slog.F("retained", len(failureParams.IDs)), slog.Error(err))
			return
		}
		failedOK, failedN = true, n
		logger.Debug(ctx, "bulk update completed",
			slog.F("updated", n), slog.F("submitted", len(failureParams.IDs)))
	}()

	wg.Wait()

	// Commit the outcome to the backlog. On error the results stay retained for a retry; on
	// success every submitted result is accounted for, either updated or stale (its lease moved
	// on), in which case dropping it is correct.
	m.resultsMu.Lock()
	defer m.resultsMu.Unlock()

	if sentOK {
		submitted := len(m.pendingSuccess)
		if stale := int64(submitted) - sentN; stale > 0 {
			m.log.Warn(ctx, "discarding stale sent results for messages re-acquired after lease expiry",
				slog.F("updated", sentN), slog.F("stale", stale))
		}
		m.metrics.SyncedUpdates.Add(float64(sentN))
		m.pendingSuccess = nil
	}
	if failedOK {
		submitted := len(m.pendingFailure)
		if stale := int64(submitted) - failedN; stale > 0 {
			m.log.Warn(ctx, "discarding stale failed results for messages re-acquired after lease expiry",
				slog.F("updated", failedN), slog.F("stale", stale))
		}
		m.metrics.SyncedUpdates.Add(float64(failedN))
		m.pendingFailure = nil
	}
}

// drainPending moves results currently buffered in the ingestion channels into the pending
// backlog. It never blocks and drains at most the number of results buffered when it starts
// (snapshot semantics): this preserves the channels' backpressure, as blocked senders filling
// freed capacity are only processed on the next drain. Results produced after the snapshot are
// picked up then, or by the final shutdown drain once producers have exited.
func (m *Manager) drainPending() {
	m.resultsMu.Lock()
	defer m.resultsMu.Unlock()

	drain := func(ch <-chan dispatchResult, pending *[]dispatchResult) {
		// Snapshot the length before reading; ranging until the channel is empty would race with
		// blocked senders parked on the channel and sweep far more results than the buffer held,
		// defeating the backpressure the buffer provides.
		for range len(ch) {
			res := <-ch
			*pending = appendDispatchResult(*pending, res)
		}
	}
	drain(m.success, &m.pendingSuccess)
	drain(m.failure, &m.pendingFailure)
}

// appendDispatchResult appends a result to the backlog, replacing any unflushed result for the
// same message. If a lease expired and the same notifier re-acquired the message before a
// previous result reached the store, the newest result is the authoritative outcome of the
// current lease.
func appendDispatchResult(results []dispatchResult, res dispatchResult) []dispatchResult {
	for i := range results {
		if results[i].msg == res.msg {
			results[i] = res
			return results
		}
	}
	return append(results, res)
}

func (m *Manager) pendingCounts() (success int, failure int) {
	m.resultsMu.Lock()
	defer m.resultsMu.Unlock()
	return len(m.pendingSuccess), len(m.pendingFailure)
}

func (m *Manager) pendingTotal() int {
	success, failure := m.pendingCounts()
	return success + failure
}

// refreshPendingGauge reflects results waiting in both the ingestion channels and the retained
// backlog (drained results that are awaiting a retry after a failed store update).
func (m *Manager) refreshPendingGauge() {
	m.metrics.pendingUpdatesGauge.set(func() int {
		return len(m.success) + len(m.failure) + m.pendingTotal()
	})
}

// Stop stops the notifier and waits until it has stopped.
func (m *Manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true

	m.log.Debug(context.Background(), "graceful stop requested")

	// If the notifier hasn't been started, we don't need to wait for anything.
	// This is only really during testing when we want to enqueue messages only but not deliver them.
	if m.notifier != nil {
		m.notifier.stop()
	}

	// Signal the stop channel to cause loop to exit.
	close(m.stop)

	if m.notifier == nil {
		return nil
	}

	m.mu.Unlock()     // Unlock to avoid blocking loop.
	defer m.mu.Lock() // Re-lock the mutex due to earlier defer.

	// Wait for the manager loop to exit or the context to be canceled, whichever comes first.
	select {
	case <-ctx.Done():
		var errStr string
		if ctx.Err() != nil {
			errStr = ctx.Err().Error()
		}
		// For some reason, slog.Error returns {} for a context error.
		m.log.Error(context.Background(), "graceful stop failed", slog.F("err", errStr))
		return ctx.Err()
	case <-m.done:
		m.log.Debug(context.Background(), "gracefully stopped")
		return nil
	}
}

type dispatchResult struct {
	notifier  uuid.UUID
	msg       uuid.UUID
	ts        time.Time
	err       error
	retryable bool
	inhibited bool
}
