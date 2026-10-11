package workspacestats

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"

	"cdr.dev/slog/v3"
	"cdr.dev/slog/v3/sloggers/slogtest"
	agentproto "github.com/coder/coder/v2/agent/proto"
	"github.com/coder/coder/v2/coderd/database"
	"github.com/coder/coder/v2/coderd/database/dbgen"
	"github.com/coder/coder/v2/coderd/database/dbtestutil"
	"github.com/coder/coder/v2/coderd/database/dbtime"
	"github.com/coder/coder/v2/coderd/database/pubsub"
	"github.com/coder/coder/v2/codersdk"
	"github.com/coder/coder/v2/cryptorand"
	"github.com/coder/coder/v2/testutil"
)

func TestBatchStats(t *testing.T) {
	t.Parallel()

	// Given: a fresh batcher with no data
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	log := slogtest.Make(t, &slogtest.Options{IgnoreErrors: true}).Leveled(slog.LevelDebug)
	store, ps := dbtestutil.NewDB(t)

	// Set up some test dependencies.
	deps1 := setupDeps(t, store, ps)
	deps2 := setupDeps(t, store, ps)
	tick := make(chan time.Time)
	flushed := make(chan int, 1)

	b, closer, err := NewBatcher(ctx,
		BatcherWithStore(store),
		BatcherWithLogger(log),
		func(b *DBBatcher) {
			b.tickCh = tick
			b.flushed = flushed
		},
	)
	require.NoError(t, err)
	t.Cleanup(closer)

	// Given: no data points are added for workspace
	// When: it becomes time to report stats
	t1 := dbtime.Now()
	// Signal a tick and wait for a flush to complete.
	tick <- t1
	f := <-flushed
	require.Equal(t, 0, f, "expected no data to be flushed")
	t.Log("flush 1 completed")

	// Then: it should report no stats.
	stats, err := store.GetWorkspaceAgentStats(ctx, t1)
	require.NoError(t, err, "should not error getting stats")
	require.Empty(t, stats, "should have no stats for workspace")

	// Given: a stat per workspace, with session counts distinct per agent so
	// that a positional misalignment on insert shows up.
	t2 := t1.Add(time.Second)
	t.Log("inserting 2 stats")
	b.Add(t2.Add(time.Millisecond), deps1.Agent.ID, deps1.Template.ID, deps1.User.ID, deps1.Workspace.ID, randStats(t, func(s *agentproto.Stats) {
		s.SessionCounts = map[string]int64{"VSCode": 3, "ssh": 1, "idle-ide": 0}
	}), false)
	b.Add(t2.Add(time.Millisecond), deps2.Agent.ID, deps2.Template.ID, deps2.User.ID, deps2.Workspace.ID, randStats(t, func(s *agentproto.Stats) {
		s.SessionCounts = map[string]int64{"jetbrains": 4, "reconnecting-pty": 2}
	}), false)

	// When: it becomes time to report stats
	// Signal a tick and wait for a flush to complete.
	tick <- t2
	f = <-flushed // Wait for a flush to complete.
	require.Equal(t, 2, f, "expected two stats to be flushed")
	t.Log("flush 2 completed")

	// Then: counts reach the right agent, normalized, without the zero entry.
	stats, err = store.GetWorkspaceAgentStats(ctx, t2)
	require.NoError(t, err, "should not error getting stats")
	require.Len(t, stats, 2, "should have stats for both workspaces")
	byAgent := make(map[uuid.UUID]database.GetWorkspaceAgentStatsRow)
	for _, stat := range stats {
		byAgent[stat.AgentID] = stat
	}
	require.EqualValues(t, 3, sessionFamilyCounts(t, byAgent[deps1.Agent.ID].SessionCounts)["vscode"])
	require.EqualValues(t, 1, sessionFamilyCounts(t, byAgent[deps1.Agent.ID].SessionCounts)["ssh"])
	require.EqualValues(t, 0, sessionFamilyCounts(t, byAgent[deps1.Agent.ID].SessionCounts)["jetbrains"])
	require.EqualValues(t, 4, sessionFamilyCounts(t, byAgent[deps2.Agent.ID].SessionCounts)["jetbrains"])
	require.EqualValues(t, 2, sessionFamilyCounts(t, byAgent[deps2.Agent.ID].SessionCounts)["reconnecting_pty"])
	require.EqualValues(t, 0, sessionFamilyCounts(t, byAgent[deps2.Agent.ID].SessionCounts)["vscode"])

	// Given: a lot of data points are added for both workspaces
	// (equal to batch size)
	t3 := t2.Add(time.Second)
	done := make(chan struct{})

	go func() {
		defer close(done)
		t.Logf("inserting %d stats", defaultBufferSize)
		for i := 0; i < defaultBufferSize; i++ {
			if i%2 == 0 {
				b.Add(t3.Add(time.Millisecond), deps1.Agent.ID, deps1.Template.ID, deps1.User.ID, deps1.Workspace.ID, randStats(t), false)
			} else {
				b.Add(t3.Add(time.Millisecond), deps2.Agent.ID, deps2.Template.ID, deps2.User.ID, deps2.Workspace.ID, randStats(t), false)
			}
		}
	}()

	// When: the buffer comes close to capacity
	// Then: The buffer will force-flush once.
	f = <-flushed
	t.Log("flush 3 completed")
	require.Greater(t, f, 819, "expected at least 819 stats to be flushed (>=80% of buffer)")
	// And we should finish inserting the stats
	<-done

	stats, err = store.GetWorkspaceAgentStats(ctx, t3)
	require.NoError(t, err, "should not error getting stats")
	require.Len(t, stats, 2, "should have stats for both workspaces")

	// Ensures that a subsequent flush pushes all the remaining data
	t4 := t3.Add(time.Second)
	tick <- t4
	f2 := <-flushed
	t.Log("flush 4 completed")
	expectedCount := defaultBufferSize - f
	require.Equal(t, expectedCount, f2, "did not flush expected remaining rows")

	// Ensure that a subsequent flush does not push stale data.
	t5 := t4.Add(time.Second)
	tick <- t5
	f = <-flushed
	require.Zero(t, f, "expected zero stats to have been flushed")
	t.Log("flush 5 completed")

	stats, err = store.GetWorkspaceAgentStats(ctx, t5)
	require.NoError(t, err, "should not error getting stats")
	require.Len(t, stats, 0, "should have no stats for workspace")

	// Ensure that buf never grew beyond what we expect
	require.Equal(t, defaultBufferSize, cap(b.buf.ID), "buffer grew beyond expected capacity")
}

// TestBatchStatsSessionCountFold asserts that a report over the session count
// cap folds the excess into the overflow counter and logs it at debug, never
// at warn, so one misbehaving agent cannot flood the logs.
func TestBatchStatsSessionCountFold(t *testing.T) {
	t.Parallel()

	ctx := testutil.Context(t, testutil.WaitLong)
	sink := testutil.NewFakeSink(t)
	store, ps := dbtestutil.NewDB(t)
	deps := setupDeps(t, store, ps)

	b, closer, err := NewBatcher(ctx,
		BatcherWithStore(store),
		BatcherWithLogger(sink.Logger(slog.LevelDebug)),
		func(b *DBBatcher) {
			// Take control of flushes so they do not interleave with the
			// assertions below.
			b.tickCh = make(chan time.Time)
		},
	)
	require.NoError(t, err)
	t.Cleanup(closer)

	const extra = 6
	st := randStats(t, func(s *agentproto.Stats) {
		s.SessionCounts = make(map[string]int64, maxSessionCountEntries+extra)
		for i := range maxSessionCountEntries + extra {
			s.SessionCounts[fmt.Sprintf("app_%d", i)] = 1
		}
	})
	b.Add(dbtime.Now(), deps.Agent.ID, deps.Template.ID, deps.User.ID, deps.Workspace.ID, st, false)

	overcap := sink.Entries(func(e slog.SinkEntry) bool {
		return strings.Contains(e.Message, "too many distinct session types")
	})
	require.Len(t, overcap, 1)
	require.Equal(t, slog.LevelDebug, overcap[0].Level)
	require.Equal(t, float64(extra), prom_testutil.ToFloat64(b.metrics.SessionCountsOverflowTotal))
}

// randStats returns a random agentproto.Stats
func randStats(t *testing.T, opts ...func(*agentproto.Stats)) *agentproto.Stats {
	t.Helper()
	s := &agentproto.Stats{
		ConnectionsByProto: map[string]int64{
			"ssh":              mustRandInt64n(t, 9) + 1,
			"vscode":           mustRandInt64n(t, 9) + 1,
			"jetbrains":        mustRandInt64n(t, 9) + 1,
			"reconnecting_pty": mustRandInt64n(t, 9) + 1,
		},
		ConnectionCount:             mustRandInt64n(t, 99) + 1,
		ConnectionMedianLatencyMs:   float64(mustRandInt64n(t, 99) + 1),
		RxPackets:                   mustRandInt64n(t, 99) + 1,
		RxBytes:                     mustRandInt64n(t, 99) + 1,
		TxPackets:                   mustRandInt64n(t, 99) + 1,
		TxBytes:                     mustRandInt64n(t, 99) + 1,
		SessionCountVscode:          mustRandInt64n(t, 9) + 1,
		SessionCountJetbrains:       mustRandInt64n(t, 9) + 1,
		SessionCountReconnectingPty: mustRandInt64n(t, 9) + 1,
		SessionCountSsh:             mustRandInt64n(t, 9) + 1,
		Metrics:                     []*agentproto.Stats_Metric{},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// deps is a set of test dependencies.
type deps struct {
	Agent     database.WorkspaceAgent
	Template  database.Template
	User      database.User
	Workspace database.WorkspaceTable
}

// setupDeps sets up a set of test dependencies.
// It creates an organization, user, template, workspace, and agent
// along with all the other miscellaneous plumbing required to link
// them together.
func setupDeps(t *testing.T, store database.Store, ps pubsub.Pubsub) deps {
	t.Helper()

	org := dbgen.Organization(t, store, database.Organization{})
	user := dbgen.User(t, store, database.User{})
	_, err := store.InsertOrganizationMember(context.Background(), database.InsertOrganizationMemberParams{
		OrganizationID: org.ID,
		UserID:         user.ID,
		Roles:          []string{codersdk.RoleOrganizationMember},
	})
	require.NoError(t, err)
	tv := dbgen.TemplateVersion(t, store, database.TemplateVersion{
		OrganizationID: org.ID,
		CreatedBy:      user.ID,
	})
	tpl := dbgen.Template(t, store, database.Template{
		CreatedBy:       user.ID,
		OrganizationID:  org.ID,
		ActiveVersionID: tv.ID,
	})
	ws := dbgen.Workspace(t, store, database.WorkspaceTable{
		TemplateID:     tpl.ID,
		OwnerID:        user.ID,
		OrganizationID: org.ID,
		LastUsedAt:     time.Now().Add(-time.Hour),
	})
	pj := dbgen.ProvisionerJob(t, store, ps, database.ProvisionerJob{
		InitiatorID:    user.ID,
		OrganizationID: org.ID,
	})
	_ = dbgen.WorkspaceBuild(t, store, database.WorkspaceBuild{
		TemplateVersionID: tv.ID,
		WorkspaceID:       ws.ID,
		JobID:             pj.ID,
	})
	res := dbgen.WorkspaceResource(t, store, database.WorkspaceResource{
		Transition: database.WorkspaceTransitionStart,
		JobID:      pj.ID,
	})
	agt := dbgen.WorkspaceAgent(t, store, database.WorkspaceAgent{
		ResourceID: res.ID,
	})
	return deps{
		Agent:     agt,
		Template:  tpl,
		User:      user,
		Workspace: ws,
	}
}

// mustRandInt64n returns a random int64 in the range [0, n).
func mustRandInt64n(t *testing.T, n int64) int64 {
	t.Helper()
	i, err := cryptorand.Intn(int(n))
	require.NoError(t, err)
	return int64(i)
}

func sessionFamilyCounts(t *testing.T, data json.RawMessage) map[codersdk.AppFamilyName]int64 {
	t.Helper()
	counts, err := codersdk.DecodeAppMap[int64](data)
	require.NoError(t, err)
	return codersdk.SumByFamily(counts)
}

// errInjectedStatsUnavailable simulates a transient database outage.
var errInjectedStatsUnavailable = xerrors.New("injected: workspace agent stats unavailable")

// flakyStore fails the first failFirst InsertWorkspaceAgentStats calls, then
// passes through to the wrapped store.
type flakyStore struct {
	database.Store
	inserts   atomic.Int64
	failFirst atomic.Int64

	mu       sync.Mutex
	captured []database.InsertWorkspaceAgentStatsParams
}

func (s *flakyStore) InsertWorkspaceAgentStats(ctx context.Context, arg database.InsertWorkspaceAgentStatsParams) error {
	if s.inserts.Add(1) <= s.failFirst.Load() {
		return errInjectedStatsUnavailable
	}
	s.mu.Lock()
	s.captured = append(s.captured, arg)
	s.mu.Unlock()
	return s.Store.InsertWorkspaceAgentStats(ctx, arg)
}

func (s *flakyStore) snapshotCaptured() []database.InsertWorkspaceAgentStatsParams {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]database.InsertWorkspaceAgentStatsParams, len(s.captured))
	copy(out, s.captured)
	return out
}

// newManualBatcher wires a batcher whose flushes are driven by the returned
// tick channel, with the retry timer parked far in the future so tests stay
// deterministic.
func newManualBatcher(t *testing.T, store database.Store, spool string) (*DBBatcher, func(), chan time.Time, chan int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	log := slogtest.Make(t, &slogtest.Options{IgnoreErrors: true}).Leveled(slog.LevelDebug)
	tick := make(chan time.Time)
	// Generous buffer so a flush never waits on the test reading a signal.
	flushed := make(chan int, 32)
	b, closer, err := NewBatcher(ctx,
		BatcherWithStore(store),
		BatcherWithLogger(log),
		BatcherWithSpoolDir(spool),
		func(b *DBBatcher) {
			b.tickCh = tick
			b.flushed = flushed
			b.retryMinBackoff = time.Hour
			b.retryMaxBackoff = time.Hour
		},
	)
	require.NoError(t, err)
	return b, closer, tick, flushed
}

func waitFlushed(t *testing.T, flushed <-chan int) int {
	t.Helper()
	select {
	case n := <-flushed:
		return n
	case <-time.After(testutil.WaitShort):
		t.Fatal("timed out waiting for agent stats flush")
		return 0
	}
}

func triggerFlush(t *testing.T, tick chan<- time.Time, flushed <-chan int) int {
	t.Helper()
	select {
	case tick <- dbtime.Now():
	case <-time.After(testutil.WaitShort):
		t.Fatal("timed out triggering agent stats flush")
	}
	return waitFlushed(t, flushed)
}

type agentStatsTotals struct {
	count    int
	distinct int
	rxBytes  int64
}

func queryAgentStatsTotals(t *testing.T, sqlDB *sql.DB, since time.Time) agentStatsTotals {
	t.Helper()
	ctx := testutil.Context(t, testutil.WaitShort)
	var totals agentStatsTotals
	err := sqlDB.QueryRowContext(ctx, `
		SELECT count(*), count(DISTINCT id), COALESCE(SUM(rx_bytes), 0)
		FROM workspace_agent_stats
		WHERE created_at >= $1`, since).Scan(&totals.count, &totals.distinct, &totals.rxBytes)
	require.NoError(t, err)
	return totals
}

func spoolJSONFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var names []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), spoolSuffix) {
			names = append(names, entry.Name())
		}
	}
	return names
}

// statsFor returns one deterministic stats report. JSON session counts and the
// deprecated fixed fields are exercised independently by the caller.
func statsFor(rx int64, mutate func(*agentproto.Stats)) *agentproto.Stats {
	st := &agentproto.Stats{
		ConnectionsByProto:        map[string]int64{"ssh": 1},
		ConnectionCount:           1,
		ConnectionMedianLatencyMs: 5,
		RxPackets:                 1,
		RxBytes:                   rx,
		TxPackets:                 1,
		TxBytes:                   1,
		Metrics:                   []*agentproto.Stats_Metric{},
	}
	if mutate != nil {
		mutate(st)
	}
	return st
}

func jsonSessionStats(rx int64, counts map[string]int64) *agentproto.Stats {
	return statsFor(rx, func(s *agentproto.Stats) {
		s.SessionCounts = counts
	})
}

func legacySessionStats(rx int64, vscode, jetbrains, pty, ssh int64) *agentproto.Stats {
	// Deliberately no SessionCounts map: pre-v2.11 agents set only the fixed
	// fields.
	return statsFor(rx, func(s *agentproto.Stats) {
		s.SessionCountVscode = vscode
		s.SessionCountJetbrains = jetbrains
		s.SessionCountReconnectingPty = pty
		s.SessionCountSsh = ssh
	})
}

// assertLatestSessions loads the latest row per agent and asserts the session
// map reached it after both normalization paths (JSON map and fixed fields).
func assertLatestSessions(t *testing.T, store database.Store, since time.Time, want map[uuid.UUID]map[codersdk.AppFamilyName]int64) {
	t.Helper()
	ctx := testutil.Context(t, testutil.WaitShort)
	stats, err := store.GetWorkspaceAgentStats(ctx, since)
	require.NoError(t, err)
	byAgent := make(map[uuid.UUID]database.GetWorkspaceAgentStatsRow, len(stats))
	for _, stat := range stats {
		byAgent[stat.AgentID] = stat
	}
	for agentID, families := range want {
		row, ok := byAgent[agentID]
		require.True(t, ok, "missing stats row for agent %s", agentID)
		require.Equal(t, families, sessionFamilyCounts(t, row.SessionCounts), "agent %s sessions", agentID)
	}
}

func rollupUsageMins(t *testing.T, store database.Store, since time.Time, templateIDs []uuid.UUID) map[uuid.UUID]int16 {
	t.Helper()
	ctx := testutil.Context(t, testutil.WaitShort)
	require.NoError(t, store.UpsertTemplateUsageStats(ctx))
	rows, err := store.GetTemplateUsageStats(ctx, database.GetTemplateUsageStatsParams{
		StartTime:   since.Add(-time.Hour),
		EndTime:     dbtime.Now().Add(time.Hour),
		TemplateIDs: templateIDs,
	})
	require.NoError(t, err)
	mins := make(map[uuid.UUID]int16, len(templateIDs))
	for _, row := range rows {
		mins[row.TemplateID] += row.UsageMins
	}
	return mins
}

// TestBatchStatsFlushOnClose asserts that a graceful stop flushes reports that
// are still only in memory, without waiting for another tick.
func TestBatchStatsFlushOnClose(t *testing.T) {
	t.Parallel()

	store, ps, sqlDB := dbtestutil.NewDBWithSQLDB(t)
	deps1 := setupDeps(t, store, ps)
	deps2 := setupDeps(t, store, ps)
	since := dbtime.Now()
	now := since.Add(time.Millisecond)

	b, closer, _, _ := newManualBatcher(t, store, t.TempDir())
	b.Add(now, deps1.Agent.ID, deps1.Template.ID, deps1.User.ID, deps1.Workspace.ID,
		jsonSessionStats(100, map[string]int64{"VSCode": 3}), false)
	b.Add(now, deps2.Agent.ID, deps2.Template.ID, deps2.User.ID, deps2.Workspace.ID,
		legacySessionStats(200, 0, 4, 0, 0), false)

	// No flush tick is ever sent.
	closer()

	totals := queryAgentStatsTotals(t, sqlDB, since)
	require.Equal(t, 2, totals.count, "both in-memory reports must be written on close")
	require.Equal(t, 2, totals.distinct, "each report keeps its own ID")

	assertLatestSessions(t, store, since, map[uuid.UUID]map[codersdk.AppFamilyName]int64{
		deps1.Agent.ID: {codersdk.AppFamilyVSCode: 3},
		deps2.Agent.ID: {codersdk.AppFamilyJetBrains: 4},
	})
}

// TestBatchStatsRetryAfterDBFailure verifies that a complete failed batch is
// retained and retried after the database recovers, that later reports are not
// blocked, that the retry never double-counts, and that the rows reach the
// template usage rollup.
func TestBatchStatsRetryAfterDBFailure(t *testing.T) {
	t.Parallel()

	ctx := testutil.Context(t, testutil.WaitLong)
	store, ps, sqlDB := dbtestutil.NewDBWithSQLDB(t)
	deps1 := setupDeps(t, store, ps)
	deps2 := setupDeps(t, store, ps)
	since := dbtime.Now()
	now := since.Add(time.Millisecond)
	// Later reports carry a strictly later timestamp so the "latest row per
	// agent" insight query has no tie to break.
	nowLater := since.Add(2 * time.Millisecond)

	flaky := &flakyStore{Store: store}
	flaky.failFirst.Store(1) // First insert attempt fails, later ones succeed.
	spool := t.TempDir()
	b, closer, tick, flushed := newManualBatcher(t, flaky, spool)

	b.Add(now, deps1.Agent.ID, deps1.Template.ID, deps1.User.ID, deps1.Workspace.ID,
		jsonSessionStats(100, map[string]int64{"vscode": 3}), false)
	b.Add(now, deps2.Agent.ID, deps2.Template.ID, deps2.User.ID, deps2.Workspace.ID,
		legacySessionStats(200, 0, 4, 0, 0), false)

	// First flush: the database is unavailable. The batch stays complete and
	// durable, nothing reaches the table.
	n := triggerFlush(t, tick, flushed)
	require.Equal(t, 2, n)
	require.Equal(t, agentStatsTotals{count: 0, distinct: 0, rxBytes: 0}, queryAgentStatsTotals(t, sqlDB, since))
	require.Len(t, spoolJSONFiles(t, spool), 1, "failed batch must be spooled")

	// A report accepted after the failure joins the retry independently.
	b.Add(nowLater, deps1.Agent.ID, deps1.Template.ID, deps1.User.ID, deps1.Workspace.ID,
		jsonSessionStats(10, map[string]int64{"ssh": 1}), false)

	// Second flush: outage over. The old batch and the new report land in
	// order, exactly once.
	n = triggerFlush(t, tick, flushed)
	require.Equal(t, 3, n)
	totals := queryAgentStatsTotals(t, sqlDB, since)
	require.Equal(t, 3, totals.count)
	require.Equal(t, 3, totals.distinct, "replayed batch must not duplicate rows")
	require.EqualValues(t, 310, totals.rxBytes)
	require.Empty(t, spoolJSONFiles(t, spool), "spool file removed after successful insert")

	// Replaying the exact same batch (as recovery would after a crash between
	// insert and spool removal) inserts nothing new.
	captured := flaky.snapshotCaptured()
	require.Len(t, captured, 2, "one call for the retried batch, one for the new report")
	require.NoError(t, store.InsertWorkspaceAgentStats(ctx, captured[0]))
	require.Equal(t, totals, queryAgentStatsTotals(t, sqlDB, since))

	// Both normalization paths survive, and every accepted minute reaches the
	// template usage summary.
	assertLatestSessions(t, store, since, map[uuid.UUID]map[codersdk.AppFamilyName]int64{
		deps1.Agent.ID: {codersdk.AppFamilySSH: 1},
		deps2.Agent.ID: {codersdk.AppFamilyJetBrains: 4},
	})
	mins := rollupUsageMins(t, store, since, []uuid.UUID{deps1.Template.ID, deps2.Template.ID})
	require.Equal(t, map[uuid.UUID]int16{
		deps1.Template.ID: 1,
		deps2.Template.ID: 1,
	}, mins)

	closer()
}

// TestBatchStatsSpoolRecovery restarts the batcher while the database is
// down: spooled batches replay on the next start, and a repeated replay does
// not produce duplicate stats.
func TestBatchStatsSpoolRecovery(t *testing.T) {
	t.Parallel()

	store, ps, sqlDB := dbtestutil.NewDBWithSQLDB(t)
	deps1 := setupDeps(t, store, ps)
	deps2 := setupDeps(t, store, ps)
	since := dbtime.Now()
	now := since.Add(time.Millisecond)
	spool := t.TempDir()

	// First process: every insert fails.
	down := &flakyStore{Store: store}
	down.failFirst.Store(1_000_000)
	b1, closer1, tick1, flushed1 := newManualBatcher(t, down, spool)
	b1.Add(now, deps1.Agent.ID, deps1.Template.ID, deps1.User.ID, deps1.Workspace.ID,
		jsonSessionStats(100, map[string]int64{"VSCode": 3}), false)
	b1.Add(now, deps2.Agent.ID, deps2.Template.ID, deps2.User.ID, deps2.Workspace.ID,
		legacySessionStats(200, 0, 4, 2, 0), false)
	require.Equal(t, 2, triggerFlush(t, tick1, flushed1))
	require.Equal(t, 0, queryAgentStatsTotals(t, sqlDB, since).count)

	// Grab the spooled bytes to simulate a later double recovery.
	files := spoolJSONFiles(t, spool)
	require.Len(t, files, 1)
	spooled, err := os.ReadFile(filepath.Join(spool, files[0]))
	require.NoError(t, err)

	// Graceful stop while the database is still down: the batch is already on
	// disk and must not be duplicated there.
	closer1()
	require.Len(t, spoolJSONFiles(t, spool), 1)
	require.Equal(t, 0, queryAgentStatsTotals(t, sqlDB, since).count)

	// Restart against a healthy database on the same spool directory. The
	// batch is replayed immediately, before any tick.
	_, closer2, _, flushed2 := newManualBatcher(t, store, spool)
	require.Equal(t, 2, waitFlushed(t, flushed2))
	totals := queryAgentStatsTotals(t, sqlDB, since)
	require.Equal(t, 2, totals.count)
	require.Equal(t, 2, totals.distinct)
	require.EqualValues(t, 300, totals.rxBytes)
	require.Empty(t, spoolJSONFiles(t, spool))

	assertLatestSessions(t, store, since, map[uuid.UUID]map[codersdk.AppFamilyName]int64{
		deps1.Agent.ID: {codersdk.AppFamilyVSCode: 3},
		deps2.Agent.ID: {codersdk.AppFamilyJetBrains: 4, codersdk.AppFamilyReconnectingPTY: 2},
	})
	closer2()

	// Simulate a crash after the rows landed but before the spool file was
	// removed: the next start replays the exact same IDs and inserts nothing.
	_, err = writeSpoolFile(spool, spooled)
	require.NoError(t, err)
	require.Len(t, spoolJSONFiles(t, spool), 1)
	_, closer3, _, flushed3 := newManualBatcher(t, store, spool)
	require.Equal(t, 2, waitFlushed(t, flushed3))
	closer3()

	require.Equal(t, totals, queryAgentStatsTotals(t, sqlDB, since), "repeated recovery must not double-count")
	require.Empty(t, spoolJSONFiles(t, spool), "replayed spool file is removed even when it was a no-op")
}

// TestBatchStatsCorruptSpoolQuarantined verifies that an unreadable spool
// batch is quarantined and does not block recovery of the others or any later
// reports.
func TestBatchStatsCorruptSpoolQuarantined(t *testing.T) {
	t.Parallel()

	store, ps, sqlDB := dbtestutil.NewDBWithSQLDB(t)
	deps := setupDeps(t, store, ps)
	since := dbtime.Now()
	now := since.Add(time.Millisecond)
	spool := t.TempDir()

	// Unreadable batch.
	require.NoError(t, os.WriteFile(filepath.Join(spool, "000-corrupt"+spoolSuffix), []byte("{not json"), 0o600))
	// Temp file abandoned by a process that died mid-write.
	require.NoError(t, os.WriteFile(filepath.Join(spool, ".001-stale"+spoolSuffix+tmpSuffix), []byte("x"), 0o600))
	// One intact batch.
	params := database.InsertWorkspaceAgentStatsParams{
		ID:                        []uuid.UUID{uuid.New()},
		CreatedAt:                 []time.Time{now},
		UserID:                    []uuid.UUID{deps.User.ID},
		WorkspaceID:               []uuid.UUID{deps.Workspace.ID},
		TemplateID:                []uuid.UUID{deps.Template.ID},
		AgentID:                   []uuid.UUID{deps.Agent.ID},
		ConnectionsByProto:        json.RawMessage(`[{"ssh":1}]`),
		ConnectionCount:           []int64{1},
		RxPackets:                 []int64{1},
		RxBytes:                   []int64{7},
		TxPackets:                 []int64{1},
		TxBytes:                   []int64{1},
		SessionCounts:             json.RawMessage(`[{"vscode":2}]`),
		ConnectionMedianLatencyMS: []float64{5},
		Usage:                     []bool{false},
	}
	data, err := json.Marshal(params)
	require.NoError(t, err)
	_, err = writeSpoolFile(spool, data)
	require.NoError(t, err)

	b, closer, tick, flushed := newManualBatcher(t, store, spool)

	// Startup replays only the intact batch; the corrupt one inserted nothing.
	require.Equal(t, 1, waitFlushed(t, flushed))
	totals := queryAgentStatsTotals(t, sqlDB, since)
	require.Equal(t, 1, totals.count)
	require.EqualValues(t, 7, totals.rxBytes)

	// Corrupt file is quarantined, stale temp removed, intact batch consumed.
	entries, err := os.ReadDir(spool)
	require.NoError(t, err)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	require.ElementsMatch(t, []string{"000-corrupt" + spoolSuffix + badSuffix}, names)

	// Fresh reports are accepted and written despite the corrupt batch.
	b.Add(now, deps.Agent.ID, deps.Template.ID, deps.User.ID, deps.Workspace.ID,
		jsonSessionStats(9, map[string]int64{"ssh": 1}), false)
	require.Equal(t, 1, triggerFlush(t, tick, flushed))
	totals = queryAgentStatsTotals(t, sqlDB, since)
	require.Equal(t, 2, totals.count)
	require.Equal(t, 2, totals.distinct)

	closer()
}

// TestBatchStatsConcurrentReports hammers Add from many goroutines while the
// first few inserts fail, and asserts that every accepted report reaches the
// table exactly once and the rollup summary matches.
func TestBatchStatsConcurrentReports(t *testing.T) {
	t.Parallel()

	ctx := testutil.Context(t, testutil.WaitLong)
	store, ps, sqlDB := dbtestutil.NewDBWithSQLDB(t)
	deps1 := setupDeps(t, store, ps)
	deps2 := setupDeps(t, store, ps)
	since := dbtime.Now()
	now := since.Add(time.Millisecond)
	spool := t.TempDir()

	// The outage clears after three failed insert calls.
	flaky := &flakyStore{Store: store}
	flaky.failFirst.Store(3)

	log := slogtest.Make(t, &slogtest.Options{IgnoreErrors: true}).Leveled(slog.LevelDebug)
	b, closer, err := NewBatcher(ctx,
		BatcherWithStore(flaky),
		BatcherWithLogger(log),
		BatcherWithSpoolDir(spool),
		BatcherWithInterval(15*time.Millisecond),
	)
	require.NoError(t, err)

	const reporters = 10
	const perReporter = 20
	const total = reporters * perReporter

	var wg sync.WaitGroup
	for r := 0; r < reporters; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < perReporter; i++ {
				idx := r*perReporter + i
				d := deps1
				if idx%2 == 1 {
					d = deps2
				}
				b.Add(now, d.Agent.ID, d.Template.ID, d.User.ID, d.Workspace.ID,
					jsonSessionStats(int64(idx+1), map[string]int64{"vscode": 1}), false)
			}
		}(r)
	}
	wg.Wait()

	require.Eventually(t, func() bool {
		return queryAgentStatsTotals(t, sqlDB, since).count == total
	}, testutil.WaitLong, testutil.IntervalFast, "all concurrent reports must land after the outage")

	totals := queryAgentStatsTotals(t, sqlDB, since)
	require.Equal(t, total, totals.count)
	require.Equal(t, total, totals.distinct, "must lose and duplicate nothing")
	require.EqualValues(t, total*(total+1)/2, totals.rxBytes)
	require.Empty(t, spoolJSONFiles(t, spool), "all spooled batches must be consumed")

	mins := rollupUsageMins(t, store, since, []uuid.UUID{deps1.Template.ID, deps2.Template.ID})
	require.Equal(t, map[uuid.UUID]int16{
		deps1.Template.ID: 1,
		deps2.Template.ID: 1,
	}, mins, "all accepted reports share one minute bucket per template")

	closer()
}
