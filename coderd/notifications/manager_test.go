package notifications_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"

	"cdr.dev/slog/v3"
	"cdr.dev/slog/v3/sloggers/slogtest"
	"github.com/coder/coder/v2/coderd/database"
	"github.com/coder/coder/v2/coderd/database/dbauthz"
	"github.com/coder/coder/v2/coderd/database/dbgen"
	"github.com/coder/coder/v2/coderd/database/dbtestutil"
	"github.com/coder/coder/v2/coderd/notifications"
	"github.com/coder/coder/v2/coderd/notifications/dispatch"
	"github.com/coder/coder/v2/coderd/notifications/types"
	"github.com/coder/coder/v2/testutil"
	"github.com/coder/quartz"
	"github.com/coder/serpent"
)

func TestBufferedUpdates(t *testing.T) {
	t.Parallel()

	// setup

	store, ps := dbtestutil.NewDB(t)
	logger := testutil.Logger(t)

	interceptor := &syncInterceptor{Store: store}
	santa := &santaHandler{}
	santaInbox := &santaHandler{}

	cfg := defaultNotificationsConfig(database.NotificationMethodSmtp)
	cfg.StoreSyncInterval = serpent.Duration(time.Hour) // Ensure we don't sync the store automatically.

	// GIVEN: a manager which will pass or fail notifications based on their "nice" labels
	mgr, err := notifications.NewManager(cfg, interceptor, ps, defaultHelpers(), createMetrics(), logger.Named("notifications-manager"))
	require.NoError(t, err)

	handlers := map[database.NotificationMethod]notifications.Handler{
		database.NotificationMethodSmtp:  santa,
		database.NotificationMethodInbox: santaInbox,
	}

	mgr.WithHandlers(handlers)
	enq, err := notifications.NewStoreEnqueuer(cfg, interceptor, defaultHelpers(), logger.Named("notifications-enqueuer"), quartz.NewReal())
	require.NoError(t, err)

	user := dbgen.User(t, store, database.User{})

	// WHEN: notifications are enqueued which should succeed and fail
	ctx := testutil.Context(t, testutil.WaitSuperLong)
	_, err = enq.Enqueue(ctx, user.ID, notifications.TemplateWorkspaceDeleted, map[string]string{"nice": "true", "i": "0"}, "") // Will succeed.
	require.NoError(t, err)
	_, err = enq.Enqueue(ctx, user.ID, notifications.TemplateWorkspaceDeleted, map[string]string{"nice": "true", "i": "1"}, "") // Will succeed.
	require.NoError(t, err)
	_, err = enq.Enqueue(ctx, user.ID, notifications.TemplateWorkspaceDeleted, map[string]string{"nice": "false", "i": "2"}, "") // Will fail.
	require.NoError(t, err)

	mgr.Run(ctx)

	// THEN:

	const (
		expectedSuccess = 2
		expectedFailure = 1
	)

	// Wait for messages to be dispatched.
	require.Eventually(t, func() bool {
		return santa.naughty.Load() == expectedFailure &&
			santa.nice.Load() == expectedSuccess
	}, testutil.WaitMedium, testutil.IntervalFast)

	// Wait for the expected number of buffered updates to be accumulated.
	require.Eventually(t, func() bool {
		success, failure := mgr.BufferedUpdatesCount()
		return success == expectedSuccess*len(handlers) && failure == expectedFailure*len(handlers)
	}, testutil.WaitShort, testutil.IntervalFast)

	// Stop the manager which forces an update of buffered updates.
	require.NoError(t, mgr.Stop(ctx))

	// Wait until both success & failure updates have been sent to the store.
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		if err := interceptor.err.Load(); err != nil {
			ct.Errorf("bulk update encountered error: %s", err)
			// Panic when an unexpected error occurs.
			ct.FailNow()
		}

		assert.EqualValues(ct, expectedFailure*len(handlers), interceptor.failed.Load())
		assert.EqualValues(ct, expectedSuccess*len(handlers), interceptor.sent.Load())
	}, testutil.WaitMedium, testutil.IntervalFast)
}

func TestBuildPayload(t *testing.T) {
	t.Parallel()

	// SETUP

	store, _ := dbtestutil.NewDB(t)
	logger := testutil.Logger(t)

	// GIVEN: a set of helpers to be injected into the templates
	const label = "Click here!"
	const baseURL = "http://xyz.com"
	const url = baseURL + "/@bobby/my-workspace"
	helpers := map[string]any{
		"my_label": func() string { return label },
		"my_url":   func() string { return baseURL },
	}

	// GIVEN: an enqueue interceptor which returns mock metadata
	interceptor := newEnqueueInterceptor(store,
		// Inject custom message metadata to influence the payload construction.
		func() database.FetchNewMessageMetadataRow {
			// Inject template actions which use injected help functions.
			actions := []types.TemplateAction{
				{
					Label: "{{ my_label }}",
					URL:   "{{ my_url }}/@{{.UserName}}/{{.Labels.name}}",
				},
			}
			out, err := json.Marshal(actions)
			assert.NoError(t, err)

			return database.FetchNewMessageMetadataRow{
				NotificationName: "My Notification",
				Actions:          out,
				UserID:           uuid.New(),
				UserEmail:        "bob@bob.com",
				UserName:         "bobby",
			}
		})

	enq, err := notifications.NewStoreEnqueuer(defaultNotificationsConfig(database.NotificationMethodSmtp), interceptor, helpers, logger.Named("notifications-enqueuer"), quartz.NewReal())
	require.NoError(t, err)

	// WHEN: a notification is enqueued
	ctx := testutil.Context(t, testutil.WaitSuperLong)
	_, err = enq.Enqueue(ctx, uuid.New(), notifications.TemplateWorkspaceDeleted, map[string]string{
		"name": "my-workspace",
	}, "test")
	require.NoError(t, err)

	// THEN: expect that a payload will be constructed and have the expected values
	payload := testutil.TryReceive(ctx, t, interceptor.payload)
	require.Len(t, payload.Actions, 1)
	require.Equal(t, label, payload.Actions[0].Label)
	require.Equal(t, url, payload.Actions[0].URL)
}

func TestStopBeforeRun(t *testing.T) {
	t.Parallel()

	// SETUP

	store, ps := dbtestutil.NewDB(t)
	logger := testutil.Logger(t)

	// GIVEN: a standard manager
	mgr, err := notifications.NewManager(defaultNotificationsConfig(database.NotificationMethodSmtp), store, ps, defaultHelpers(), createMetrics(), logger.Named("notifications-manager"))
	require.NoError(t, err)

	// THEN: validate that the manager can be stopped safely without Run() having been called yet
	ctx := testutil.Context(t, testutil.WaitSuperLong)
	require.Eventually(t, func() bool {
		assert.NoError(t, mgr.Stop(ctx))
		return true
	}, testutil.WaitShort, testutil.IntervalFast)
}

func TestRunStopRace(t *testing.T) {
	t.Parallel()

	// SETUP

	store, ps := dbtestutil.NewDB(t)
	logger := testutil.Logger(t)

	// GIVEN: a standard manager
	mgr, err := notifications.NewManager(defaultNotificationsConfig(database.NotificationMethodSmtp), store, ps, defaultHelpers(), createMetrics(), logger.Named("notifications-manager"))
	require.NoError(t, err)

	// Start Run and Stop after each other (run does "go loop()").
	// This is to catch a (now fixed) race condition where the manager
	// would be accessed/stopped while it was being created/starting up.
	ctx := testutil.Context(t, testutil.WaitMedium)
	mgr.Run(ctx)
	err = mgr.Stop(ctx)
	require.NoError(t, err)
}

type syncInterceptor struct {
	notifications.Store

	sent   atomic.Int32
	failed atomic.Int32
	err    atomic.Value
}

func (b *syncInterceptor) BulkMarkNotificationMessagesSent(ctx context.Context, arg database.BulkMarkNotificationMessagesSentParams) (int64, error) {
	updated, err := b.Store.BulkMarkNotificationMessagesSent(ctx, arg)
	// #nosec G115 - Safe conversion as the count of updated notification messages is expected to be within int32 range
	b.sent.Add(int32(updated))
	if err != nil {
		b.err.Store(err)
	}
	return updated, err
}

func (b *syncInterceptor) BulkMarkNotificationMessagesFailed(ctx context.Context, arg database.BulkMarkNotificationMessagesFailedParams) (int64, error) {
	updated, err := b.Store.BulkMarkNotificationMessagesFailed(ctx, arg)
	// #nosec G115 - Safe conversion as the count of updated notification messages is expected to be within int32 range
	b.failed.Add(int32(updated))
	if err != nil {
		b.err.Store(err)
	}
	return updated, err
}

// santaHandler only dispatches nice messages.
type santaHandler struct {
	naughty atomic.Int32
	nice    atomic.Int32
}

func (s *santaHandler) Dispatcher(payload types.MessagePayload, _, _ string, _ template.FuncMap) (dispatch.DeliveryFunc, error) {
	return func(_ context.Context, _ uuid.UUID) (retryable bool, err error) {
		if payload.Labels["nice"] != "true" {
			s.naughty.Add(1)
			return false, xerrors.New("be nice")
		}

		s.nice.Add(1)
		return false, nil
	}, nil
}

type enqueueInterceptor struct {
	notifications.Store

	payload    chan types.MessagePayload
	metadataFn func() database.FetchNewMessageMetadataRow
}

func newEnqueueInterceptor(db notifications.Store, metadataFn func() database.FetchNewMessageMetadataRow) *enqueueInterceptor {
	return &enqueueInterceptor{Store: db, payload: make(chan types.MessagePayload, 2), metadataFn: metadataFn}
}

func (e *enqueueInterceptor) EnqueueNotificationMessage(_ context.Context, arg database.EnqueueNotificationMessageParams) error {
	var payload types.MessagePayload
	err := json.Unmarshal(arg.Payload, &payload)
	if err != nil {
		return err
	}

	e.payload <- payload
	return err
}

func (e *enqueueInterceptor) FetchNewMessageMetadata(_ context.Context, _ database.FetchNewMessageMetadataParams) (database.FetchNewMessageMetadataRow, error) {
	return e.metadataFn(), nil
}

// outageTestLogger returns a test logger which ignores the expected "synthetic store outage"
// errors produced by the failure-injecting test stores, while still failing on any other error.
func outageTestLogger(t *testing.T) slog.Logger {
	t.Helper()
	return slogtest.Make(t, &slogtest.Options{IgnoreErrorFn: func(entry slog.SinkEntry) bool {
		if testutil.IgnoreLoggedError(entry) {
			return true
		}
		err, ok := slogtest.FindFirstError(entry)
		return ok && strings.Contains(err.Error(), "synthetic store outage")
	}}).Leveled(slog.LevelDebug)
}

// flakySyncStore fails the first configured bulk update calls before recovering. Results the
// manager retains while the store is failing must be retried afterwards and converge.
type flakySyncStore struct {
	notifications.Store

	mu                     sync.Mutex
	failSent, failFailed   int
	sentCalls, failedCalls int
}

func (s *flakySyncStore) BulkMarkNotificationMessagesSent(ctx context.Context, arg database.BulkMarkNotificationMessagesSentParams) (int64, error) {
	s.mu.Lock()
	s.sentCalls++
	fail := s.failSent > 0
	if fail {
		s.failSent--
	}
	s.mu.Unlock()

	if fail {
		return 0, xerrors.New("synthetic store outage: mark notification messages sent")
	}
	return s.Store.BulkMarkNotificationMessagesSent(ctx, arg)
}

func (s *flakySyncStore) BulkMarkNotificationMessagesFailed(ctx context.Context, arg database.BulkMarkNotificationMessagesFailedParams) (int64, error) {
	s.mu.Lock()
	s.failedCalls++
	fail := s.failFailed > 0
	if fail {
		s.failFailed--
	}
	s.mu.Unlock()

	if fail {
		return 0, xerrors.New("synthetic store outage: mark notification messages failed")
	}
	return s.Store.BulkMarkNotificationMessagesFailed(ctx, arg)
}

func messagesByStatus(t *testing.T, ctx context.Context, store notifications.Store, status database.NotificationMessageStatus) []database.NotificationMessage {
	t.Helper()
	rows, err := store.GetNotificationMessagesByStatus(ctx, database.GetNotificationMessagesByStatusParams{
		Status: status,
		Limit:  100,
	})
	require.NoError(t, err)
	return rows
}

// TestBufferedUpdatesRetainedAndFlushedAfterStoreOutage verifies that dispatch results which
// cannot be written because the bulk update temporarily fails are retained and retried, rather
// than dropped: every message converges to its final state, its original failure reason is
// preserved, and attempt accounting is not double-applied by the retry.
func TestBufferedUpdatesRetainedAndFlushedAfterStoreOutage(t *testing.T) {
	t.Parallel()

	// SETUP

	store, ps := dbtestutil.NewDB(t)
	logger := outageTestLogger(t)

	// Fail the first few bulk update attempts to simulate a transient database outage.
	flaky := &flakySyncStore{Store: store, failSent: 3, failFailed: 3}

	cfg := defaultNotificationsConfig(database.NotificationMethodSmtp)
	cfg.FetchInterval = serpent.Duration(time.Millisecond * 50)
	cfg.StoreSyncInterval = serpent.Duration(time.Millisecond * 50)
	cfg.RetryInterval = serpent.Duration(time.Hour) // Dispatch-level retries must not interfere.

	mgr, err := notifications.NewManager(cfg, flaky, ps, defaultHelpers(), createMetrics(), logger.Named("notifications-manager"))
	require.NoError(t, err)
	smtp := &santaHandler{}
	inbox := &santaHandler{}
	mgr.WithHandlers(map[database.NotificationMethod]notifications.Handler{
		database.NotificationMethodSmtp:  smtp,
		database.NotificationMethodInbox: inbox,
	})
	enq, err := notifications.NewStoreEnqueuer(cfg, flaky, defaultHelpers(), logger.Named("notifications-enqueuer"), quartz.NewReal())
	require.NoError(t, err)

	user := dbgen.User(t, store, database.User{})
	ctx := dbauthz.AsNotifier(testutil.Context(t, testutil.WaitSuperLong))

	// WHEN: two messages succeed and one fails permanently (once per enabled method).
	for i, nice := range []string{"true", "true", "false"} {
		_, err = enq.Enqueue(ctx, user.ID, notifications.TemplateWorkspaceDeleted,
			map[string]string{"nice": nice, "i": fmt.Sprintf("%d", i)}, "")
		require.NoError(t, err)
	}
	mgr.Run(ctx)

	// THEN: despite the initial store outage every result is eventually persisted exactly once.
	require.Eventually(t, func() bool {
		sent := messagesByStatus(t, ctx, store, database.NotificationMessageStatusSent)
		failed := messagesByStatus(t, ctx, store, database.NotificationMessageStatusPermanentFailure)
		return len(sent) == 4 && len(failed) == 2
	}, testutil.WaitLong, testutil.IntervalFast)

	require.NoError(t, mgr.Stop(ctx))

	// THEN: the retained backlog is fully reconciled.
	pendingSuccess, pendingFailure := mgr.BufferedUpdatesCount()
	require.Zero(t, pendingSuccess)
	require.Zero(t, pendingFailure)

	// THEN: each message accounts for exactly one attempt; the bulk-update retries did not burn
	// an additional attempt, and the original failure reason survived to the final state.
	for _, msg := range messagesByStatus(t, ctx, store, database.NotificationMessageStatusSent) {
		require.Equal(t, int32(1), msg.AttemptCount.Int32)
		require.False(t, msg.StatusReason.Valid)
	}
	for _, msg := range messagesByStatus(t, ctx, store, database.NotificationMessageStatusPermanentFailure) {
		require.Equal(t, int32(1), msg.AttemptCount.Int32)
		require.Equal(t, "be nice", msg.StatusReason.String)
	}
}

// gatedSyncStore blocks all "mark sent" bulk updates until its release channel is closed.
type gatedSyncStore struct {
	notifications.Store

	releaseSent chan struct{}
	sentCalls   atomic.Int32
}

func (g *gatedSyncStore) BulkMarkNotificationMessagesSent(ctx context.Context, arg database.BulkMarkNotificationMessagesSentParams) (int64, error) {
	g.sentCalls.Add(1)
	select {
	case <-g.releaseSent:
		return g.Store.BulkMarkNotificationMessagesSent(ctx, arg)
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// TestSentAndFailureBulkUpdatesAreIndependent verifies that a stalled bulk update for one kind of
// result does not block the other kind, and that sync ticks do not pile duplicate bulk updates
// onto a stalled store.
func TestSentAndFailureBulkUpdatesAreIndependent(t *testing.T) {
	t.Parallel()

	// SETUP

	store, ps := dbtestutil.NewDB(t)
	logger := testutil.Logger(t)

	gated := &gatedSyncStore{Store: store, releaseSent: make(chan struct{})}

	cfg := defaultNotificationsConfig(database.NotificationMethodSmtp)
	cfg.FetchInterval = serpent.Duration(time.Millisecond * 50)
	cfg.StoreSyncInterval = serpent.Duration(time.Millisecond * 50)
	cfg.RetryInterval = serpent.Duration(time.Hour)

	mgr, err := notifications.NewManager(cfg, gated, ps, defaultHelpers(), createMetrics(), logger.Named("notifications-manager"))
	require.NoError(t, err)
	mgr.WithHandlers(map[database.NotificationMethod]notifications.Handler{
		database.NotificationMethodSmtp:  &santaHandler{},
		database.NotificationMethodInbox: &santaHandler{},
	})
	enq, err := notifications.NewStoreEnqueuer(cfg, gated, defaultHelpers(), logger.Named("notifications-enqueuer"), quartz.NewReal())
	require.NoError(t, err)

	user := dbgen.User(t, store, database.User{})
	ctx := dbauthz.AsNotifier(testutil.Context(t, testutil.WaitSuperLong))

	// WHEN: one message succeeds and one fails permanently.
	_, err = enq.Enqueue(ctx, user.ID, notifications.TemplateWorkspaceDeleted, map[string]string{"nice": "true"}, "")
	require.NoError(t, err)
	_, err = enq.Enqueue(ctx, user.ID, notifications.TemplateWorkspaceDeleted, map[string]string{"nice": "false"}, "")
	require.NoError(t, err)
	mgr.Run(ctx)

	// THEN: failure results are written even though the "mark sent" bulk update is stalled.
	require.Eventually(t, func() bool {
		return len(messagesByStatus(t, ctx, store, database.NotificationMessageStatusPermanentFailure)) == 2
	}, testutil.WaitMedium, testutil.IntervalFast)
	require.Empty(t, messagesByStatus(t, ctx, store, database.NotificationMessageStatusSent))

	// THEN: ticks arriving while the first update is stalled do not queue duplicate store calls.
	// (Wait across several sync intervals; only the in-flight call should exist.)
	require.Eventually(t, func() bool {
		return gated.sentCalls.Load() == 1
	}, testutil.WaitShort, testutil.IntervalFast)
	time.Sleep(testutil.IntervalMedium)
	require.EqualValues(t, 1, gated.sentCalls.Load())

	// WHEN: the store recovers.
	close(gated.releaseSent)

	// THEN: the retained sent results converge as well.
	require.Eventually(t, func() bool {
		return len(messagesByStatus(t, ctx, store, database.NotificationMessageStatusSent)) == 2
	}, testutil.WaitMedium, testutil.IntervalFast)
	require.NoError(t, mgr.Stop(ctx))
}

// TestBulkUpdatesOnlyApplyToLeaseOwner verifies the store-level ownership guard that prevents
// stale buffered results from overwriting a state confirmed by a later dispatch after lease
// re-acquisition.
func TestBulkUpdatesOnlyApplyToLeaseOwner(t *testing.T) {
	t.Parallel()

	ctx := dbauthz.AsNotifier(testutil.Context(t, testutil.WaitSuperLong))
	store, _ := dbtestutil.NewDB(t)
	logger := testutil.Logger(t)

	cfg := defaultNotificationsConfig(database.NotificationMethodSmtp)
	enq, err := notifications.NewStoreEnqueuer(cfg, store, defaultHelpers(), logger.Named("notifications-enqueuer"), quartz.NewReal())
	require.NoError(t, err)

	user := dbgen.User(t, store, database.User{})
	ids, err := enq.Enqueue(ctx, user.ID, notifications.TemplateWorkspaceDeleted, map[string]string{"nice": "true"}, "")
	require.NoError(t, err)
	require.Len(t, ids, 2)

	// Acquire the messages as notifier A.
	ownerA := uuid.New()
	leased, err := store.AcquireNotificationMessages(ctx, database.AcquireNotificationMessagesParams{
		// #nosec G115 - Test values are well within int32 range.
		Count:           int32(cfg.LeaseCount.Value()),
		MaxAttemptCount: int32(cfg.MaxSendAttempts.Value()), // #nosec G115
		NotifierID:      ownerA,
		LeaseSeconds:    60,
	})
	require.NoError(t, err)
	require.Len(t, leased, 2)

	now := time.Now()

	// THEN: stale results attributed to a different notifier apply to no rows.
	n, err := store.BulkMarkNotificationMessagesSent(ctx, database.BulkMarkNotificationMessagesSentParams{
		IDs:         ids,
		SentAts:     []time.Time{now, now},
		NotifierIDs: []uuid.UUID{uuid.New(), uuid.New()},
	})
	require.NoError(t, err)
	require.EqualValues(t, 0, n)

	// THEN: the rows are untouched and still leased by A.
	leasedRows := messagesByStatus(t, ctx, store, database.NotificationMessageStatusLeased)
	require.Len(t, leasedRows, 2)
	for _, row := range leasedRows {
		require.Equal(t, "Leased by notifier "+ownerA.String(), row.StatusReason.String)
	}

	// WHEN: the legitimate owner records a success.
	n, err = store.BulkMarkNotificationMessagesSent(ctx, database.BulkMarkNotificationMessagesSentParams{
		IDs:         ids,
		SentAts:     []time.Time{now, now},
		NotifierIDs: []uuid.UUID{ownerA, ownerA},
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, n)

	sentRows := messagesByStatus(t, ctx, store, database.NotificationMessageStatusSent)
	require.Len(t, sentRows, 2)
	for _, row := range sentRows {
		require.Equal(t, int32(1), row.AttemptCount.Int32)
	}

	// THEN: a stale failure result arriving after the confirmed "sent" state cannot overwrite it,
	// and retrying the finalization is a no-op (attempt_count is not incremented again).
	n, err = store.BulkMarkNotificationMessagesFailed(ctx, database.BulkMarkNotificationMessagesFailedParams{
		MaxAttempts:   int32(cfg.MaxSendAttempts.Value()), // #nosec G115
		RetryInterval: 60,
		IDs:           ids,
		FailedAts:     []time.Time{now, now},
		NotifierIDs:   []uuid.UUID{ownerA, ownerA},
		Statuses: []database.NotificationMessageStatus{
			database.NotificationMessageStatusTemporaryFailure,
			database.NotificationMessageStatusTemporaryFailure,
		},
		StatusReasons: []string{"late failure", "late failure"},
	})
	require.NoError(t, err)
	require.EqualValues(t, 0, n)

	sentRows = messagesByStatus(t, ctx, store, database.NotificationMessageStatusSent)
	require.Len(t, sentRows, 2)
	for _, row := range sentRows {
		require.Equal(t, database.NotificationMessageStatusSent, row.Status)
		require.Equal(t, int32(1), row.AttemptCount.Int32)
		require.False(t, row.StatusReason.Valid)
	}
	require.Empty(t, messagesByStatus(t, ctx, store, database.NotificationMessageStatusTemporaryFailure))
}

// TestStopFlushesBufferedResultsAfterContextCanceled verifies that a graceful Stop persists
// results already produced even if the manager's run context was canceled concurrently.
func TestStopFlushesBufferedResultsAfterContextCanceled(t *testing.T) {
	t.Parallel()

	store, ps := dbtestutil.NewDB(t)
	logger := testutil.Logger(t)

	cfg := defaultNotificationsConfig(database.NotificationMethodSmtp)
	cfg.FetchInterval = serpent.Duration(time.Millisecond * 50)
	cfg.StoreSyncInterval = serpent.Duration(time.Hour) // Results stay buffered; no periodic flush.

	mgr, err := notifications.NewManager(cfg, store, ps, defaultHelpers(), createMetrics(), logger.Named("notifications-manager"))
	require.NoError(t, err)
	mgr.WithHandlers(map[database.NotificationMethod]notifications.Handler{
		database.NotificationMethodSmtp:  &santaHandler{},
		database.NotificationMethodInbox: &santaHandler{},
	})
	enq, err := notifications.NewStoreEnqueuer(cfg, store, defaultHelpers(), logger.Named("notifications-enqueuer"), quartz.NewReal())
	require.NoError(t, err)

	user := dbgen.User(t, store, database.User{})

	runCtx, cancelRun := context.WithCancel(dbauthz.AsNotifier(context.Background()))
	mgr.Run(runCtx)

	// WHEN: messages are dispatched and buffered, then the run context is canceled.
	for i := range 2 {
		_, err = enq.Enqueue(dbauthz.AsNotifier(testutil.Context(t, testutil.WaitSuperLong)), user.ID,
			notifications.TemplateWorkspaceDeleted, map[string]string{"nice": "true", "i": fmt.Sprintf("%d", i)}, "")
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		success, failure := mgr.BufferedUpdatesCount()
		return success == 4 && failure == 0
	}, testutil.WaitMedium, testutil.IntervalFast)

	cancelRun()

	// THEN: the graceful stop still flushes the buffered results.
	require.NoError(t, mgr.Stop(testutil.Context(t, testutil.WaitSuperLong)))
	require.Eventually(t, func() bool {
		return len(messagesByStatus(t, dbauthz.AsNotifier(context.Background()), store,
			database.NotificationMessageStatusSent)) == 4
	}, testutil.WaitMedium, testutil.IntervalFast)
}

// TestContextCancelBestEffortFlushConvergesAcrossRestart verifies that canceling the manager
// context persists already-produced results, and that a replacement manager observes the
// converged state without re-delivering the persisted messages.
func TestContextCancelBestEffortFlushConvergesAcrossRestart(t *testing.T) {
	t.Parallel()

	store, ps := dbtestutil.NewDB(t)
	logger := testutil.Logger(t)

	cfg := defaultNotificationsConfig(database.NotificationMethodSmtp)
	cfg.FetchInterval = serpent.Duration(time.Millisecond * 50)
	cfg.StoreSyncInterval = serpent.Duration(time.Hour) // No periodic flush; only the shutdown flush persists results.

	firstHandler := newDispatchInterceptor(&santaHandler{})
	mgr, err := notifications.NewManager(cfg, store, ps, defaultHelpers(), createMetrics(), logger.Named("manager-1"))
	require.NoError(t, err)
	mgr.WithHandlers(map[database.NotificationMethod]notifications.Handler{
		database.NotificationMethodSmtp:  firstHandler,
		database.NotificationMethodInbox: firstHandler,
	})
	enq, err := notifications.NewStoreEnqueuer(cfg, store, defaultHelpers(), logger.Named("notifications-enqueuer"), quartz.NewReal())
	require.NoError(t, err)

	user := dbgen.User(t, store, database.User{})

	runCtx, cancelRun := context.WithCancel(dbauthz.AsNotifier(context.Background()))
	mgr.Run(runCtx)

	// WHEN: three messages are dispatched (six across both methods) and buffered.
	ctx := dbauthz.AsNotifier(testutil.Context(t, testutil.WaitSuperLong))
	for i := range 3 {
		_, err = enq.Enqueue(ctx, user.ID, notifications.TemplateWorkspaceDeleted,
			map[string]string{"nice": "true", "i": fmt.Sprintf("%d", i)}, "")
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		success, failure := mgr.BufferedUpdatesCount()
		return firstHandler.sent.Load() == 6 && success == 6 && failure == 0
	}, testutil.WaitMedium, testutil.IntervalFast)

	// WHEN: the first manager is canceled ungracefully.
	cancelRun()

	// THEN: its best-effort flush still persists the already-produced results.
	require.Eventually(t, func() bool {
		return len(messagesByStatus(t, ctx, store, database.NotificationMessageStatusSent)) == 6
	}, testutil.WaitMedium, testutil.IntervalFast)
	require.Empty(t, messagesByStatus(t, ctx, store, database.NotificationMessageStatusLeased))

	// WHEN: a replacement manager takes over.
	secondHandler := newDispatchInterceptor(&santaHandler{})
	mgr2, err := notifications.NewManager(cfg, store, ps, defaultHelpers(), createMetrics(), logger.Named("manager-2"))
	require.NoError(t, err)
	mgr2.WithHandlers(map[database.NotificationMethod]notifications.Handler{
		database.NotificationMethodSmtp:  secondHandler,
		database.NotificationMethodInbox: secondHandler,
	})
	mgr2.Run(dbauthz.AsNotifier(context.Background()))
	t.Cleanup(func() {
		assert.NoError(t, mgr2.Stop(testutil.Context(t, testutil.WaitSuperLong)))
	})

	// THEN: no persisted messages are re-delivered and the observable state stays converged.
	require.Never(t, func() bool {
		return secondHandler.sent.Load() > 0
	}, time.Second, testutil.IntervalFast)
	require.Zero(t, secondHandler.err.Load())
	require.Len(t, messagesByStatus(t, ctx, store, database.NotificationMessageStatusSent), 6)
	require.Empty(t, messagesByStatus(t, ctx, store, database.NotificationMessageStatusLeased))
}
