package job

// Offline tests for missed-run recovery.
//
// Everything here uses a mock clock, MemoryDB, and fake backends. No real
// database, process signal, or wall-clock sleep is used: time is advanced
// explicitly and scheduling decisions are inspected directly.

import (
	"errors"
	"testing"
	"time"

	"github.com/mixer/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scheduledJob builds a loaded job with an explicit interval.
func recoveryJob(repeat string, start time.Time, interval string) *Job {
	j := GetMockJob()
	j.Id = "recovery-job"
	j.Schedule = repeat + "/" + start.UTC().Format(time.RFC3339) + "/" + interval
	return j
}

// fakeBackend returns a fixed set of jobs or an errored partial load.
type fakeBackend struct {
	jobs     []*Job
	err      error
	loadCall int
}

func (f *fakeBackend) GetAll() ([]*Job, error)     { f.loadCall++; return f.jobs, f.err }
func (f *fakeBackend) Get(id string) (*Job, error) { return nil, ErrJobNotFound(id) }
func (f *fakeBackend) Delete(id string) error      { return nil }
func (f *fakeBackend) Save(job *Job) error         { return nil }
func (f *fakeBackend) Close() error                { return nil }

// newRecoveryCache returns a LockFreeJobCache driven by a mock clock.
func newRecoveryCache(t *testing.T, db JobDB) (*LockFreeJobCache, *clock.MockClock) {
	t.Helper()
	clk := clock.NewMockClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	cache := NewLockFreeJobCache(db)
	cache.Clock.SetClock(clk)
	return cache, clk
}

// TestRecoveryStartNinetyMinutesDefault exercises the real Start path: a
// one-minute job stopped for ninety minutes catches up once and then waits for
// the next whole minute.
func TestRecoveryStartNinetyMinutesDefault(t *testing.T) {
	clk := clock.NewMockClock(time.Date(2026, 1, 1, 1, 30, 0, 0, time.UTC)) // 90 minutes after start
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	j := GetMockJob()
	j.Id = "ninety"
	j.Schedule = "R/" + start.UTC().Format(time.RFC3339) + "/PT1M"
	j.succeedInstantly = true
	j.LastScheduledRunPoint = start // the start point itself was consumed before the stop
	// InitDelayDuration writes scheduleTime/delayDuration; do it directly so
	// the fake backend does not need to.

	var delayOK = j.InitDelayDuration(false)
	require.NoError(t, delayOK)
	// Re-apply the watermark: InitDelayDuration does not clear it, but be
	// explicit about the fixture's intent.
	j.LastScheduledRunPoint = start

	cache := NewLockFreeJobCache(&fakeBackend{jobs: []*Job{j}})
	cache.Clock.SetClock(clk)
	report := loadForTest(t, cache)

	assert.Equal(t, 1, report.Compensated, "one catch-up run is planned")
	require.Eventually(t, func() bool {
		j.lock.RLock()
		defer j.lock.RUnlock()
		return j.Metadata.CatchUpPlanned == 0 && len(j.Stats) == 1
	}, 2*time.Second, 10*time.Millisecond)

	j.lock.RLock()
	assert.Equal(t, TriggerCatchUp, j.Stats[0].Trigger)
	next := j.NextRunAt
	j.lock.RUnlock()
	assert.Equal(t, 0, next.Second(), "back to the next whole minute")
	assert.Equal(t, time.Date(2026, 1, 1, 1, 31, 0, 0, time.UTC), next.UTC())
}

// loadForTest calls Start and returns its report.
func loadForTest(t *testing.T, cache *LockFreeJobCache) LoadReport {
	t.Helper()
	cache.Start(0, -1)
	return cache.LoadReport()
}

// TestRecoveryLegacyDefaults drains the policy table.
func TestResolveMissedRunPolicy(t *testing.T) {
	cases := []struct {
		name        string
		policy      MissedRunPolicy
		resumeAt    bool
		max         int
		wantPolicy  MissedRunPolicy
		wantCatchUp int
	}{
		{"unset, resume=false means catch up once", "", false, 0, MissedRunPolicyCatchUp, 1},
		{"unset, resume=true means none", "", true, 0, MissedRunPolicyNone, 0},
		{"none wins", MissedRunPolicyNone, false, 5, MissedRunPolicyNone, 0},
		{"catch_up default cap is one", MissedRunPolicyCatchUp, false, 0, MissedRunPolicyCatchUp, 1},
		{"catch_up respects a positive cap", MissedRunPolicyCatchUp, true, 10, MissedRunPolicyCatchUp, 10},
		{"catch_up ignores a non-positive cap", MissedRunPolicyCatchUp, true, -3, MissedRunPolicyCatchUp, 1},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			j := &Job{MissedRunPolicy: c.policy, ResumeAtNextScheduledTime: c.resumeAt, MaxMissedRuns: c.max}
			j.lock.Lock()
			p, n := j.resolvedMissedRunPolicyLocked()
			j.lock.Unlock()
			assert.Equal(t, c.wantPolicy, p)
			assert.Equal(t, c.wantCatchUp, n)
		})
	}
}

// A restart after a long outage must not replay every missed point.
func TestRecoveryDowntimeOfOneWeekIsCapped(t *testing.T) {
	now := time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC) // one week after start
	start := now.Add(-7 * 24 * time.Hour)

	j := recoveryJob("R", start, "PT1M")
	require.NoError(t, j.InitDelayDuration(false))
	j.succeedInstantly = true
	j.Metadata.CatchUpPlanned = 1 // default cap, as a fresh episode
	j.LastScheduledRunPoint = start

	plan := j.planRecoveryLocked(now)
	// The cap is one; the missed count is huge but bounded by the grid scan.
	assert.Equal(t, 1, plan.planned)
	assert.True(t, plan.result.Truncated, "a week of minutes must be truncated to the cap")
	assert.Greater(t, plan.missed, 9000, "about 10080 points were missed; they must not all run")
}

// Default policy: ninety minutes of downtime on a one-minute job runs exactly
// once, then returns to the next whole minute.
func TestRecoveryNinetyMinutesDefaultCatchUpOnceThenGrid(t *testing.T) {
	cache, clk := newRecoveryCache(t, NewMemoryDB())
	start := clk.Now()

	j := recoveryJob("R", start, "PT1M")
	j.clk.SetClock(clk)
	require.NoError(t, j.InitDelayDuration(false))
	j.succeedInstantly = true
	j.LastScheduledRunPoint = start // last processed point
	require.NoError(t, cache.Set(j))

	clk.AddTime(90 * time.Minute)

	j.runCatchUpEpisode(cache)
	require.Eventually(t, func() bool { return j.Metadata.CatchUpPlanned == 0 }, time.Second, 10*time.Millisecond)

	require.NotNil(t, j.Metadata.LastCatchUpResult)
	assert.Equal(t, 1, j.Metadata.LastCatchUpResult.Executed, "exactly one catch-up run")
	assert.Equal(t, 1, len(j.Stats))
	assert.Equal(t, TriggerCatchUp, j.Stats[0].Trigger)

	// Normal scheduling then waits for the next whole minute, not
	// "one interval after the catch-up run".
	j.lock.RLock()
	next := j.NextRunAt
	j.lock.RUnlock()
	assert.Equal(t, 0, next.Second(), "next point is a whole minute")
	assert.Equal(t, time.Date(2026, 1, 1, 1, 31, 0, 0, time.UTC), next.UTC())
}

// catch_up with a cap of ten over ninety missed points runs ten, not ninety.
func TestRecoveryCapCatchUpTen(t *testing.T) {
	cache, clk := newRecoveryCache(t, NewMemoryDB())
	start := clk.Now()

	j := recoveryJob("R", start, "PT1M")
	j.clk.SetClock(clk)
	require.NoError(t, j.InitDelayDuration(false))
	j.succeedInstantly = true
	j.MissedRunPolicy = MissedRunPolicyCatchUp
	j.MaxMissedRuns = 10
	j.LastScheduledRunPoint = start
	require.NoError(t, cache.Set(j))

	clk.AddTime(90 * time.Minute)

	j.runCatchUpEpisode(cache)
	require.Eventually(t, func() bool { return j.Metadata.CatchUpPlanned == 0 }, 2*time.Second, 10*time.Millisecond)

	assert.Equal(t, 10, len(j.Stats))
	require.NotNil(t, j.Metadata.LastCatchUpResult)
	assert.Equal(t, 90, j.Metadata.LastCatchUpResult.MissedPoints)
	assert.Equal(t, 10, j.Metadata.LastCatchUpResult.Planned)
	assert.Equal(t, 10, j.Metadata.LastCatchUpResult.Executed)
	assert.True(t, j.Metadata.LastCatchUpResult.Truncated)
}

// policy "none" never compensates and waits for the next whole point.
func TestRecoveryNoneWaitsForNextPoint(t *testing.T) {
	j := recoveryJob("R", time.Now().Add(-90*time.Minute), "PT1M")
	require.NoError(t, j.InitDelayDuration(false))
	j.MissedRunPolicy = MissedRunPolicyNone
	j.LastScheduledRunPoint = time.Now().Add(-91 * time.Minute)

	plan := j.planRecoveryLocked(time.Now())
	assert.Equal(t, 0, plan.planned)
}

// An explicit policy overrides ResumeAtNextScheduledTime.
func TestRecoveryExplicitPolicyBeatsResumeFlag(t *testing.T) {
	j := recoveryJob("R", time.Now().Add(-90*time.Minute), "PT1M")
	require.NoError(t, j.InitDelayDuration(false))
	j.MissedRunPolicy = MissedRunPolicyCatchUp
	j.MaxMissedRuns = 3
	j.ResumeAtNextScheduledTime = true
	j.LastScheduledRunPoint = time.Now().Add(-91 * time.Minute)
	plan := j.planRecoveryLocked(time.Now())
	assert.Equal(t, 3, plan.planned)
}

// A crash during recovery continues the remaining budget; it must not
// recompute the outage as ninety runs.
func TestRecoveryInterruptedContinuesBudget(t *testing.T) {
	now := time.Now()
	j := recoveryJob("R", now.Add(-90*time.Minute), "PT1M")
	require.NoError(t, j.InitDelayDuration(false))
	j.MissedRunPolicy = MissedRunPolicyCatchUp
	j.MaxMissedRuns = 10
	j.LastScheduledRunPoint = now.Add(-91 * time.Minute)

	// Simulate: an episode of 10 planned was persisted, 4 executed, then the
	// process died.
	j.Metadata.CatchUpPlanned = 10
	j.Metadata.CatchUpRuns = 4
	j.Metadata.LastCatchUpResult = &CatchUpResult{MissedPoints: 90, Planned: 10}

	plan := j.planRecoveryLocked(now)
	assert.True(t, plan.continue_, "a persisted budget means the episode continues")
	assert.Equal(t, 6, plan.planned, "only the remaining budget is scheduled")
}

// A finite job whose stats were truncated by jobstat-ttl must not run again.
func TestRecoveryFiniteJobSurvivesStatsTruncation(t *testing.T) {
	j := loadedJob(t, "R2/"+rfc3339Ago(time.Hour)+"/PT1H")
	j.Metadata.ScheduledRuns = 3 // R2 means 3 runs total
	j.Stats = nil                // retention removed everything
	assert.False(t, j.ShouldStartWaiting(), "the independent counter must stop it")
}

// A legacy job with no ScheduledRuns falls back to NumberOfFinishedRuns.
func TestRecoveryLegacyCounterFallback(t *testing.T) {
	j := loadedJob(t, "R2/"+rfc3339Ago(time.Hour)+"/PT1H")
	j.Metadata.NumberOfFinishedRuns = 3
	j.Stats = nil
	assert.False(t, j.ShouldStartWaiting())
}

// A disabled job loaded at startup is not scheduled, but enabling it does arm
// it (fixing the v1 bug where a timer had to survive).
func TestRecoveryEnableAfterRestartArmsTimer(t *testing.T) {
	clk := clock.NewMockClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	start := clk.Now().Add(time.Hour)

	j := recoveryJob("R", start, "PT1H")
	j.Disabled = true
	require.NoError(t, j.InitDelayDuration(false))

	fake := &fakeBackend{jobs: []*Job{j}}
	cache := NewLockFreeJobCache(fake)
	cache.Clock.SetClock(clk)
	cache.Start(0, -1)

	// Loaded but not scheduled.
	j.lock.RLock()
	assert.Nil(t, j.jobTimer)
	j.lock.RUnlock()

	require.NoError(t, cache.Enable(j))
	require.Eventually(t, func() bool {
		j.lock.RLock()
		defer j.lock.RUnlock()
		return j.jobTimer != nil
	}, time.Second, 10*time.Millisecond, "enabling a loaded-disabled job must arm its timer")
	assert.False(t, j.Disabled)
}

// A child without its own schedule waits for the dependency and is loaded but
// not armed.
func TestRecoveryChildWithoutScheduleWaitsForDependency(t *testing.T) {
	clk := clock.NewMockClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	parent := GetMockJob()
	parent.Id = "parent"
	parent.Schedule = "R/" + clk.Now().Add(time.Hour).UTC().Format(time.RFC3339) + "/PT1H"
	require.NoError(t, parent.InitDelayDuration(false))

	child := GetMockJob()
	child.Id = "child"
	child.ParentJobs = []string{"parent"}
	// No schedule.

	fake := &fakeBackend{jobs: []*Job{parent, child}}
	cache := NewLockFreeJobCache(fake)
	cache.Clock.SetClock(clk)
	cache.Start(0, -1)

	c, err := cache.Get("child")
	require.NoError(t, err, "the child stays cache-visible")
	c.lock.RLock()
	assert.Nil(t, c.jobTimer, "a child without a schedule must not be armed")
	c.lock.RUnlock()

	// The dependency still triggers it.
	child.run(cache, TriggerDependency, time.Time{}, false)
	child.lock.RLock()
	assert.Equal(t, uint(1), child.Metadata.NumberOfFinishedRuns)
	child.lock.RUnlock()
}

// A job with both a schedule and a parent is armed from its schedule and can
// also be triggered by the dependency. Neither source consumes the other's
// work, and a dependency run never advances the schedule watermark.
func TestRecoveryScheduledJobWithParentHasBothSources(t *testing.T) {
	clk := clock.NewMockClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	parent := GetMockJob()
	parent.Id = "parent"
	parent.Schedule = "R/" + clk.Now().Add(time.Hour).UTC().Format(time.RFC3339) + "/PT1H"
	require.NoError(t, parent.InitDelayDuration(false))

	child := GetMockJob()
	child.Id = "child"
	child.ParentJobs = []string{"parent"}
	child.Schedule = "R/" + clk.Now().Add(time.Hour).UTC().Format(time.RFC3339) + "/PT1H"
	require.NoError(t, child.InitDelayDuration(false))

	fake := &fakeBackend{jobs: []*Job{parent, child}}
	cache := NewLockFreeJobCache(fake)
	cache.Clock.SetClock(clk)
	cache.Start(0, -1)

	require.Eventually(t, func() bool {
		child.lock.RLock()
		defer child.lock.RUnlock()
		return child.jobTimer != nil
	}, time.Second, 10*time.Millisecond, "the schedule source still arms the job")

	child.lock.RLock()
	watermark := child.LastScheduledRunPoint
	child.lock.RUnlock()

	child.run(cache, TriggerDependency, time.Time{}, false)
	child.lock.RLock()
	assert.Equal(t, uint(1), child.Metadata.NumberOfFinishedRuns)
	assert.Equal(t, uint(0), child.Metadata.ScheduledRuns, "a dependency run must not consume the schedule")
	assert.Equal(t, watermark, child.LastScheduledRunPoint, "a dependency run must not advance the schedule point")
	child.lock.RUnlock()
}

// A stored one-off that never ran is resumed; one that finished is not.
func TestRecoveryOneOffWithoutSchedule(t *testing.T) {
	t.Run("no schedule and no evidence runs once", func(t *testing.T) {
		clk := clock.NewMockClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		j := GetMockJob()
		j.Id = "oneoff"
		j.succeedInstantly = true

		cache := NewLockFreeJobCache(&fakeBackend{jobs: []*Job{j}})
		cache.Clock.SetClock(clk)
		cache.Start(0, -1)

		require.Eventually(t, func() bool {
			j.lock.RLock()
			defer j.lock.RUnlock()
			return j.Metadata.NumberOfFinishedRuns == 1
		}, time.Second, 10*time.Millisecond)
	})

	t.Run("no schedule and finished is not re-run", func(t *testing.T) {
		clk := clock.NewMockClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		j := GetMockJob()
		j.Id = "done-oneoff"
		j.succeedInstantly = true
		j.IsDone = true
		j.Metadata.NumberOfFinishedRuns = 1

		cache := NewLockFreeJobCache(&fakeBackend{jobs: []*Job{j}})
		cache.Clock.SetClock(clk)
		cache.Start(0, -1)

		time.Sleep(50 * time.Millisecond)
		j.lock.RLock()
		assert.Equal(t, uint(1), j.Metadata.NumberOfFinishedRuns, "a finished one-off must not run again")
		j.lock.RUnlock()
	})
}

// A bad stored job is isolated: the others load, the count is reported, and a
// whole-backend read failure is not reported as "zero bad jobs".
func TestRecoveryBadJobIsolation(t *testing.T) {
	good := GetMockJob()
	good.Id = "good"
	good.Schedule = "R/" + time.Now().Add(time.Hour).UTC().Format(time.RFC3339) + "/PT1H"
	require.NoError(t, good.InitDelayDuration(false))

	bad := GetMockJob()
	bad.Id = "bad"
	bad.Schedule = "not-a-schedule"

	db := &fakeBackend{
		jobs: []*Job{good},
		err: &JobsPartiallyLoadedError{
			Jobs:     []*Job{good},
			Failures: []JobLoadFailure{{Id: "bad", Err: errors.New("bad schedule")}},
		},
	}
	cache := NewLockFreeJobCache(db)
	cache.Start(0, -1)

	report := cache.LoadReport()
	assert.Equal(t, 1, report.Loaded, "the good job still loads")
	assert.Equal(t, 1, report.Failed, "the bad job is counted")
	_, err := cache.Get("bad")
	assert.Error(t, err, "the bad job is not in the cache")
	_, err = cache.Get("good")
	assert.NoError(t, err)
}

func TestRecoveryWholeBackendFailure(t *testing.T) {
	db := &fakeBackend{err: errors.New("database is down")}
	cache := NewLockFreeJobCache(db)
	cache.Start(0, -1)

	report := cache.LoadReport()
	require.Error(t, report.ReadError, "a whole-backend failure must be distinguishable from zero bad jobs")
	assert.Equal(t, 0, report.Loaded)
	assert.Equal(t, 0, report.Failed)
	assert.Equal(t, 0, len(cache.GetAll().Jobs))

	stats := NewKalaStats(cache)
	assert.Equal(t, 0, stats.LoadFailedJobs, "a negative failure count is not surfaced as a normal count")
}

// KalaStats exposes the failed-load count additively.
func TestRecoveryKalaStatsReportsFailures(t *testing.T) {
	j := GetMockJob()
	j.Id = "loaded"
	j.Schedule = "R/" + time.Now().Add(time.Hour).UTC().Format(time.RFC3339) + "/PT1H"
	require.NoError(t, j.InitDelayDuration(false))

	db := &fakeBackend{
		jobs: []*Job{j},
		err:  &JobsPartiallyLoadedError{Failures: []JobLoadFailure{{Id: "x", Err: errors.New("bad")}}},
	}
	cache := NewLockFreeJobCache(db)
	cache.Start(0, -1)

	assert.Equal(t, 1, NewKalaStats(cache).LoadFailedJobs)
}

// A second restart during recovery continues from the persisted points; it
// must not recompute the ninety-point outage.
func TestRecoverySecondStopDuringCatchUp(t *testing.T) {
	cache, clk := newRecoveryCache(t, NewMemoryDB())
	start := clk.Now()

	j := recoveryJob("R", start, "PT1M")
	j.clk.SetClock(clk)
	require.NoError(t, j.InitDelayDuration(false))
	j.succeedInstantly = true
	j.MissedRunPolicy = MissedRunPolicyCatchUp
	j.MaxMissedRuns = 10
	j.LastScheduledRunPoint = start
	require.NoError(t, cache.Set(j))

	clk.AddTime(90 * time.Minute)

	// Pretend the process died after 4 of 10 catch-up runs were persisted.
	j.Metadata.CatchUpPlanned = 10
	j.Metadata.CatchUpRuns = 4
	j.Metadata.LastCatchUpResult = &CatchUpResult{MissedPoints: 90, Planned: 10}
	for i := 0; i < 4; i++ {
		j.Stats = append(j.Stats, &JobStat{Trigger: TriggerCatchUp})
	}
	j.LastScheduledRunPoint = start.Add(3 * time.Minute)
	j.Metadata.NumberOfFinishedRuns = 4
	j.Metadata.ScheduledRuns = 4

	// Re-load from the persisted state: the remaining budget is 6.
	j.runCatchUpEpisode(cache)
	require.Eventually(t, func() bool { return j.Metadata.CatchUpPlanned == 0 }, 2*time.Second, 10*time.Millisecond)

	assert.Equal(t, 10, len(j.Stats), "4 before the stop plus 6 after, not 90")
	require.NotNil(t, j.Metadata.LastCatchUpResult)
	assert.Equal(t, 90, j.Metadata.LastCatchUpResult.MissedPoints)

	// The watermark advanced and a further restart does not recover again.
	j.lock.Lock()
	watermark := j.LastScheduledRunPoint
	j.lock.Unlock()
	assert.False(t, j.hasMissedPointsLocked(func() time.Time {
		j.lock.RLock()
		defer j.lock.RUnlock()
		return j.clk.Time().Now()
	}()), "nothing remains outstanding after recovery")
	assert.False(t, watermark.IsZero())
}

// A finite job that used up its repetitions, with all stats removed by
// jobstat-ttl, is not restored by Start.
func TestRecoveryFinishedFiniteJobNotRestoredAfterStatsCleanup(t *testing.T) {
	clk := clock.NewMockClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	j := GetMockJob()
	j.Id = "finished"
	j.Schedule = "R2/" + clk.Now().Add(-3*time.Hour).UTC().Format(time.RFC3339) + "/PT1H"
	j.Metadata.ScheduledRuns = 3
	j.Metadata.NumberOfFinishedRuns = 3
	j.IsDone = true
	j.Stats = nil // retention removed every entry
	require.NoError(t, j.InitDelayDuration(false))

	cache := NewLockFreeJobCache(&fakeBackend{jobs: []*Job{j}})
	cache.Clock.SetClock(clk)
	cache.Start(0, time.Hour)

	j.lock.RLock()
	assert.Nil(t, j.jobTimer, "a completed finite job must not be rescheduled")
	j.lock.RUnlock()
	// It is still visible through the API.
	got, err := cache.Get("finished")
	require.NoError(t, err)
	assert.True(t, got.IsDone)
}

// A normal and a catch-up run are distinguishable in job stats.
func TestRecoveryMetadataDistinguishesTriggers(t *testing.T) {
	cache, clk := newRecoveryCache(t, NewMemoryDB())
	start := clk.Now()

	j := recoveryJob("R", start, "PT1M")
	j.clk.SetClock(clk)
	require.NoError(t, j.InitDelayDuration(false))
	j.succeedInstantly = true
	j.LastScheduledRunPoint = start
	require.NoError(t, cache.Set(j))

	clk.AddTime(2 * time.Minute)
	j.runCatchUpEpisode(cache)
	require.Eventually(t, func() bool { return j.Metadata.CatchUpPlanned == 0 }, time.Second, 10*time.Millisecond)

	require.NotEmpty(t, j.Stats)
	for _, st := range j.Stats {
		assert.Equal(t, TriggerCatchUp, st.Trigger)
		assert.False(t, st.ScheduledPoint.IsZero())
	}
}

// The catch-up result is a single replaced record, not an ever-growing list,
// even across repeated episodes.
func TestRecoveryMetadataDoesNotGrow(t *testing.T) {
	cache, clk := newRecoveryCache(t, NewMemoryDB())
	j := recoveryJob("R", clk.Now(), "PT1M")
	j.clk.SetClock(clk)
	require.NoError(t, j.InitDelayDuration(false))
	j.succeedInstantly = true
	require.NoError(t, cache.Set(j))

	for i := 0; i < 3; i++ {
		j.lock.Lock()
		// Each episode has one outstanding point: five minutes elapsed on a
		// one-minute grid, but the cap is one.
		j.LastScheduledRunPoint = j.clk.Time().Now()
		j.Metadata.CatchUpPlanned = 0
		j.Metadata.CatchUpRuns = 0
		j.lock.Unlock()
		clk.AddTime(5 * time.Minute)

		// Plan and execute synchronously so the test does not depend on
		// goroutine scheduling.
		j.lock.Lock()
		plan := j.planRecoveryLocked(j.clk.Time().Now())
		j.Metadata.LastCatchUpResult = plan.result
		j.lock.Unlock()
		for n := 0; n < plan.planned; n++ {
			j.lock.Lock()
			point, ok := j.nextOutstandingPointLocked(j.clk.Time().Now())
			j.lock.Unlock()
			if !ok {
				break
			}
			j.run(cache, TriggerCatchUp, point, true)
		}
		j.lock.Lock()
		if lastDue, ok := j.lastGridPointAtOrBeforeLocked(j.clk.Time().Now()); ok &&
			lastDue.After(j.LastScheduledRunPoint) {
			j.LastScheduledRunPoint = lastDue
		}
		j.Metadata.CatchUpPlanned = 0
		j.Metadata.CatchUpRuns = 0
		j.lock.Unlock()
		j.StopTimer()
	}
	j.lock.RLock()
	result := j.Metadata.LastCatchUpResult
	j.lock.RUnlock()
	require.NotNil(t, result)
	// A single record: the last episode's, bounded by the cap. It is one
	// pointer field, replaced each episode, not a growing slice.
	assert.LessOrEqual(t, result.Planned, defaultMaxMissedRuns)
}

// Old stored data has none of the new fields. Loading it must behave like v1:
// a job that never ran waits one interval (no immediate catch-up), and a job
// whose last run is old fires once immediately.
func TestRecoveryLegacyStoredJobDefaults(t *testing.T) {
	t.Run("never ran: waits one interval, does not run immediately", func(t *testing.T) {
		clk := clock.NewMockClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		start := clk.Now().Add(-5 * time.Hour)
		j := GetMockJob()
		j.Id = "legacy-never-ran"
		j.Schedule = "R/" + start.UTC().Format(time.RFC3339) + "/PT1H"
		// No missed_run_policy, no LastScheduledRunPoint, no metadata.
		require.NoError(t, j.InitDelayDuration(false))

		cache := NewLockFreeJobCache(&fakeBackend{jobs: []*Job{j}})
		cache.Clock.SetClock(clk)
		report := loadForTest(t, cache)

		assert.Equal(t, 0, report.Compensated, "legacy data with no run evidence does not catch up")
		require.Eventually(t, func() bool {
			j.lock.RLock()
			defer j.lock.RUnlock()
			return j.jobTimer != nil
		}, time.Second, 10*time.Millisecond)
		j.lock.RLock()
		wait := j.NextRunAt.Sub(clk.Now())
		j.lock.RUnlock()
		assert.InDelta(t, float64(time.Hour), float64(wait), float64(time.Second),
			"legacy behavior: one full interval from now")
	})

	t.Run("last run long ago: fires once immediately", func(t *testing.T) {
		clk := clock.NewMockClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		j := GetMockJob()
		j.Id = "legacy-old-run"
		j.Schedule = "R/" + clk.Now().Add(-5*time.Hour).UTC().Format(time.RFC3339) + "/PT1H"
		j.succeedInstantly = true
		j.Metadata.LastAttemptedRun = clk.Now().Add(-3 * time.Hour) // no watermark, legacy prompt
		require.NoError(t, j.InitDelayDuration(false))

		cache := NewLockFreeJobCache(&fakeBackend{jobs: []*Job{j}})
		cache.Clock.SetClock(clk)
		report := loadForTest(t, cache)

		assert.Equal(t, 1, report.Compensated, "legacy data with an old last run catches up once")
		require.Eventually(t, func() bool {
			j.lock.RLock()
			defer j.lock.RUnlock()
			return !j.catchUpInProgress && len(j.Stats) == 1
		}, 2*time.Second, 10*time.Millisecond)
	})
}

// A job interrupted mid-run (evidence of a prior attempt, but no completed
// run) is treated conservatively: its old missed points are not replayed in
// full, and the at-least-once caveat is documented rather than a false
// exactly-once guarantee.
func TestRecoveryInterruptedMidRunIsConservative(t *testing.T) {
	now := time.Now()
	j := recoveryJob("R", now.Add(-90*time.Minute), "PT1M")
	require.NoError(t, j.InitDelayDuration(false))
	// LastAttemptedRun but no completion and no watermark: the run may or may
	// not have finished before the crash.
	j.Metadata.LastAttemptedRun = now.Add(-time.Minute)

	plan := j.planRecoveryLocked(now)
	assert.Equal(t, defaultMaxMissedRuns, plan.planned, "only the default single catch-up, not the whole outage")
}
