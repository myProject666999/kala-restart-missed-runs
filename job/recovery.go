package job

// Missed-run recovery.
//
// When Kala starts, or a disabled job is re-enabled, schedule points may
// already have passed. The legacy behavior (GetWaitDuration) compensates at
// most one such point by firing immediately and then rescheduling from the last
// attempt, which drifts off the schedule grid. This file makes the behavior
// explicit, bounded and restart-safe:
//
//   - A schedule has an identity of its own: a point is (job, grid time). The
//     most recently processed point is persisted in LastScheduledRunPoint,
//     independently of the in-memory timer chain. Manual and dependency runs
//     never advance it, so a job that both has a schedule and a parent keeps
//     the two trigger sources distinct.
//   - MissedRunPolicy decides how many outstanding points are compensated:
//       ""         legacy: ResumeAtNextScheduledTime ? none : catch up once
//       "none"     never compensate; wait for the next future point
//       "catch_up" compensate up to MaxMissedRuns (default 1) points
//     An explicitly set policy wins over ResumeAtNextScheduledTime.
//   - Catch-up runs are serial, oldest point first, with no artificial gap
//     between them. The budget is episode-wide and persisted in Metadata, so a
//     crash mid-recovery continues the remaining budget instead of recomputing
//     the whole outage (a week of minutes must not become ~10000 runs).
//   - Points that come due while the episode runs are absorbed into the same
//     budget, never executed twice, because the watermark advances after every
//     completed run and is persisted with it.
//   - When the episode ends the schedule resumes on the original grid: the next
//     point is the first grid point strictly after "now", not
//     "last attempt + interval". Points the budget dropped are skipped, not
//     deferred.
//
// Epsilon only bounds the retry window of a single run. It plays no part in
// deciding how much downtime is compensated.
//
// Persistence caveat: recovery is at-least-once, never exactly-once. A point is
// only known to be consumed after the run's metadata has been persisted. If the
// process dies in the unpersisted window (--no-tx-persist, a crash before the
// post-run save, or a backend write that failed) the point may be executed
// again on the next start. Kala does not promise exactly-once execution.

import (
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// defaultMaxMissedRuns is the catch-up cap when a policy is selected but
	// no positive cap is configured. It matches the legacy "run once".
	defaultMaxMissedRuns = 1

	// maxGridScan bounds grid walking so a pathological schedule (for
	// example PT1S starting decades ago) cannot stall startup. Realistic
	// horizons, including a week of minutes, are far below it.
	maxGridScan = 2000000 //nolint:gomnd // guard rail, not a tuning knob
)

// resolvedMissedRunPolicyLocked resolves the effective policy and catch-up cap.
// An explicitly configured policy always wins over the legacy
// ResumeAtNextScheduledTime boolean.
//
// The caller must hold j.lock.
func (j *Job) resolvedMissedRunPolicyLocked() (MissedRunPolicy, int) {
	switch j.MissedRunPolicy {
	case MissedRunPolicyCatchUp:
		cap := j.MaxMissedRuns
		if cap < 1 {
			cap = defaultMaxMissedRuns
		}
		return MissedRunPolicyCatchUp, cap
	case MissedRunPolicyNone:
		return MissedRunPolicyNone, 0
	case MissedRunPolicyUnset:
		if j.ResumeAtNextScheduledTime {
			return MissedRunPolicyNone, 0
		}
		return MissedRunPolicyCatchUp, defaultMaxMissedRuns
	default:
		// Validation rejects unknown values; treat a stray one as "none".
		return MissedRunPolicyNone, 0
	}
}

// planRunsDoneLocked is the cumulative count of runs against the schedule's
// repetition budget. New data counts only schedule-driven runs (normal or
// catch-up), so manual starts and dependency runs do not consume the plan and
// retries inside a single run are never counted as extra plan runs. Legacy data
// has no ScheduledRuns, so NumberOfFinishedRuns (and then len(Stats)) is used as
// a conservative floor: over-counting can only stop a job earlier, never make a
// finished job run again.
//
// The caller must hold j.lock.
func (j *Job) planRunsDoneLocked() int64 {
	if j.Metadata.ScheduledRuns > 0 {
		return int64(j.Metadata.ScheduledRuns)
	}
	done := int64(j.Metadata.NumberOfFinishedRuns)
	if l := int64(len(j.Stats)); l > done {
		done = l
	}
	return done
}

// remainingPlanRunsLocked returns how many more runs the repetition budget
// allows, or -1 for a forever schedule.
//
// The caller must hold j.lock.
func (j *Job) remainingPlanRunsLocked() int64 {
	if !j.hasFixedRepetitions() {
		return -1
	}
	total := int64(1)
	if j.timesToRepeat > 0 {
		total = j.timesToRepeat + 1
	}
	remaining := total - j.planRunsDoneLocked()
	if remaining < 0 {
		remaining = 0
	}
	return remaining
}

// gridReadyLocked reports whether the schedule can be walked.
//
// The caller must hold j.lock.
func (j *Job) gridReadyLocked() bool {
	return j.Schedule != "" && !j.scheduleTime.IsZero() &&
		j.delayDuration != nil && !j.delayDuration.IsZero()
}

// firstGridPointAfterLocked returns the first schedule point strictly after t,
// or false when the grid is not walkable or the scan bound is hit.
//
// The caller must hold j.lock.
func (j *Job) firstGridPointAfterLocked(t time.Time) (time.Time, bool) {
	if !j.gridReadyLocked() {
		return time.Time{}, false
	}
	point := j.scheduleTime
	if point.After(t) {
		return point, true
	}
	for i := 0; i < maxGridScan; i++ {
		point = j.delayDuration.Add(point)
		if point.After(t) {
			return point, true
		}
	}
	return time.Time{}, false
}

// lastGridPointAtOrBeforeLocked returns the newest schedule point at or before
// t, or false when there is none (for example, the start is still ahead).
//
// The caller must hold j.lock.
func (j *Job) lastGridPointAtOrBeforeLocked(t time.Time) (time.Time, bool) {
	if !j.gridReadyLocked() || j.scheduleTime.After(t) {
		return time.Time{}, false
	}
	point := j.scheduleTime
	for i := 0; i < maxGridScan; i++ {
		next := j.delayDuration.Add(point)
		if next.After(t) {
			return point, true
		}
		point = next
	}
	return point, true
}

// effectiveWatermarkLocked is the point after which outstanding work is
// measured. Jobs created before LastScheduledRunPoint existed fall back to the
// last attempt time so an upgrade does not replay the whole schedule.
//
// The caller must hold j.lock.
func (j *Job) effectiveWatermarkLocked() (time.Time, bool) {
	if !j.LastScheduledRunPoint.IsZero() {
		return j.LastScheduledRunPoint, true
	}
	if !j.Metadata.LastAttemptedRun.IsZero() {
		return j.Metadata.LastAttemptedRun, true
	}
	return time.Time{}, false
}

// countsPastStartLocked reports whether the schedule's first point counts as
// missed when the job has no run evidence yet.
//
// The legacy default must reproduce v1's GetWaitDuration, where a job that
// never ran waits one full interval from now. Only an explicitly selected
// "catch_up" policy treats the past start point itself as missed.
//
// The caller must hold j.lock.
func (j *Job) countsPastStartLocked() bool {
	return j.MissedRunPolicy == MissedRunPolicyCatchUp
}

// outstandingPointsLocked collects the points that are at or before now and
// strictly after the watermark, oldest first, up to limit. It also returns the
// total number found (which may exceed limit).
//
// The caller must hold j.lock.
func (j *Job) outstandingPointsLocked(now time.Time, limit int) (points []time.Time, total int) {
	if !j.gridReadyLocked() {
		return nil, 0
	}
	watermark, hasWatermark := j.effectiveWatermarkLocked()
	if !hasWatermark && !j.countsPastStartLocked() {
		return nil, 0
	}

	point := j.scheduleTime
	if hasWatermark {
		if p, ok := j.firstGridPointAfterLocked(watermark); ok {
			point = p
		} else {
			return nil, 0
		}
	}
	for i := 0; i < maxGridScan; i++ {
		if point.After(now) {
			break
		}
		if !hasWatermark || point.After(watermark) {
			total++
			if len(points) < limit {
				points = append(points, point)
			}
		}
		next := j.delayDuration.Add(point)
		if !next.After(point) {
			break
		}
		point = next
	}
	return points, total
}

// hasMissedPointsLocked reports whether any schedule point is outstanding.
//
// The caller must hold j.lock.
func (j *Job) hasMissedPointsLocked(now time.Time) bool {
	points, _ := j.outstandingPointsLocked(now, 1)
	return len(points) > 0
}

// recoveryPlan is the decision for one catch-up episode.
type recoveryPlan struct {
	policy    MissedRunPolicy
	planned   int // runs this episode will attempt
	missed    int // outstanding points found, before the cap
	continue_ bool
	result    *CatchUpResult
}

// planRecoveryLocked decides the catch-up work for the current outstanding
// points. The schedule must be initialized.
//
// The caller must hold j.lock.
func (j *Job) planRecoveryLocked(now time.Time) recoveryPlan {
	policy, cap := j.resolvedMissedRunPolicyLocked()
	plan := recoveryPlan{policy: policy}
	if cap < 1 || !j.gridReadyLocked() {
		return plan
	}

	remaining := j.remainingPlanRunsLocked()
	if remaining == 0 {
		return plan
	}

	// A persisted positive budget means an episode was interrupted. Continue
	// with what is left instead of recomputing the outage from scratch.
	budget := cap
	plan.continue_ = j.Metadata.CatchUpPlanned > 0
	if plan.continue_ {
		budget = int(j.Metadata.CatchUpPlanned) - int(j.Metadata.CatchUpRuns)
		if budget < 0 {
			budget = 0
		}
	}
	if remaining > 0 && int64(budget) > remaining {
		budget = int(remaining)
	}
	if budget == 0 {
		return plan
	}

	points, total := j.outstandingPointsLocked(now, budget)
	plan.missed = total
	plan.planned = len(points)

	if plan.continue_ && j.Metadata.LastCatchUpResult != nil {
		plan.result = j.Metadata.LastCatchUpResult
		if plan.missed > plan.result.MissedPoints {
			plan.result.MissedPoints = plan.missed
		}
		plan.result.Planned = plan.planned
		plan.result.Truncated = plan.planned < plan.result.MissedPoints
	} else {
		plan.result = &CatchUpResult{
			StartedAt:    now,
			MissedPoints: plan.missed,
			Planned:      plan.planned,
			Truncated:    plan.planned < plan.missed,
		}
	}
	return plan
}

// normalWaitLocked computes the next normal timer target and the schedule point
// it represents.
//
// alignToGrid selects the cadence:
//   - true: the first grid point strictly after now. This is used after a
//     catch-up episode ("back to the normal cadence, wait for the next whole
//     minute") and for policy "none" (v1's ResumeAtNextScheduledTime rule).
//   - false: the legacy cadence, so ongoing schedules keep v1 behavior:
//     start time if it is still ahead, immediately for a one-off, one interval
//     from now for a job that never ran, and "last attempt + interval"
//     afterwards (firing immediately if that is already past).
//
// The caller must hold j.lock.
func (j *Job) normalWaitLocked(now time.Time, alignToGrid bool) (time.Time, time.Duration, bool) {
	if j.Schedule == "" {
		return time.Time{}, 0, false
	}
	if now.Before(j.scheduleTime) {
		return j.scheduleTime, j.scheduleTime.Sub(now), true
	}
	if j.timesToRepeat == 0 {
		// A one-off schedule whose start has passed runs once, immediately.
		return j.scheduleTime, 0, true
	}
	if alignToGrid {
		point, ok := j.firstGridPointAfterLocked(now)
		if !ok {
			return time.Time{}, 0, false
		}
		return point, point.Sub(now), true
	}

	// Legacy cadence.
	if j.Metadata.LastAttemptedRun.IsZero() {
		wait := j.delayDuration.RelativeTo(now)
		return now.Add(wait), wait, true
	}
	due := j.delayDuration.Add(j.Metadata.LastAttemptedRun)
	wait := due.Sub(now)
	if wait < 0 {
		wait = 0
	}
	return due, wait, true
}

// runCatchUpEpisode executes the planned catch-up runs serially, then resumes
// normal scheduling on the original grid.
func (j *Job) runCatchUpEpisode(cache JobCache) {
	j.lock.Lock()
	if j.catchUpInProgress || j.Disabled || j.IsDone {
		j.lock.Unlock()
		return
	}
	j.catchUpInProgress = true
	plan := j.planRecoveryLocked(j.clk.Time().Now())
	if plan.planned == 0 {
		j.catchUpInProgress = false
		j.lock.Unlock()
		j.startWaiting(cache, false, false)
		return
	}

	// Persist the episode budget before the first run, so a crash in the
	// middle continues with the remaining budget rather than starting over.
	if !plan.continue_ {
		j.Metadata.CatchUpRuns = 0
		j.Metadata.CatchUpPlanned = uint(plan.planned)
	}
	j.Metadata.LastCatchUpResult = plan.result
	j.lock.Unlock()

	log.Infof(
		"Job %s:%s recovering: policy=%q missed=%d planned=%d truncated=%t",
		j.Name, j.Id, plan.policy, plan.missed, plan.planned, plan.result.Truncated,
	)
	j.persist(cache)

	executed := 0
	for executed < plan.planned {
		j.lock.Lock()
		if j.Disabled || j.IsDone || j.catchUpCancelledByBudgetLocked() {
			j.lock.Unlock()
			break
		}
		point, ok := j.nextOutstandingPointLocked(j.clk.Time().Now())
		j.lock.Unlock()
		if !ok {
			break
		}

		if !j.run(cache, TriggerCatchUp, point, true) {
			break
		}
		executed++
	}

	j.lock.Lock()
	finish := j.clk.Time().Now()
	if plan.result != nil {
		plan.result.Executed = executed
		// Absorb points that came due during the episode: they are part of
		// the same compensation window and are not run separately later.
		if lastDue, ok := j.lastGridPointAtOrBeforeLocked(finish); ok &&
			lastDue.After(j.LastScheduledRunPoint) {
			j.LastScheduledRunPoint = lastDue
		}
		if next, ok := j.firstGridPointAfterLocked(finish); ok {
			plan.result.NextScheduledPoint = next
		}
		j.Metadata.LastCatchUpResult = plan.result
	}
	// Close the episode; a later restart begins a fresh one, but the
	// watermark already moved past the dropped points.
	j.Metadata.CatchUpPlanned = 0
	j.Metadata.CatchUpRuns = 0
	// Back to the normal cadence: wait for the next whole grid point rather
	// than rescheduling "one interval after the last catch-up run", which
	// would stay off the grid. This is also what "do not catch up" means.
	j.Metadata.NextOnGrid = true
	j.catchUpInProgress = false
	j.lock.Unlock()

	log.Infof(
		"Job %s:%s recovery finished: executed=%d of %d planned (missed %d)",
		j.Name, j.Id, executed, plan.planned, plan.missed,
	)
	j.persist(cache)
	j.startWaiting(cache, false, false)
}

// catchUpCancelledByBudgetLocked reports whether the plan is exhausted or the
// job is done. The caller must hold j.lock.
func (j *Job) catchUpCancelledByBudgetLocked() bool {
	if j.hasFixedRepetitions() && j.remainingPlanRunsLocked() <= 0 {
		return true
	}
	return false
}

// nextOutstandingPointLocked returns the oldest point that is at or before now
// and strictly after the watermark. The caller must hold j.lock.
func (j *Job) nextOutstandingPointLocked(now time.Time) (time.Time, bool) {
	points, _ := j.outstandingPointsLocked(now, 1)
	if len(points) == 0 {
		return time.Time{}, false
	}
	return points[0], true
}

// persist saves the job through the cache, logging rather than propagating
// errors: a run has already happened and the scheduler must continue.
func (j *Job) persist(cache JobCache) {
	if err := cache.Set(j); err != nil {
		log.Errorf("Job %s with id %s could not be persisted: %v", j.Name, j.Id, err)
	}
}

// hasRunEvidenceLocked reports whether there is any persisted sign that the
// job has been attempted or completed. The caller must hold j.lock.
func (j *Job) hasRunEvidenceLocked() bool {
	return j.IsDone ||
		j.Metadata.NumberOfFinishedRuns > 0 ||
		len(j.Stats) > 0 ||
		!j.Metadata.LastSuccess.IsZero() ||
		!j.Metadata.LastAttemptedRun.IsZero() ||
		!j.LastScheduledRunPoint.IsZero()
}

// validateMissedRunPolicy checks the new fields. It does not take the lock and
// is used for jobs loaded from storage as well as for new jobs.
func (j *Job) validateMissedRunPolicy() error {
	switch j.MissedRunPolicy {
	case MissedRunPolicyUnset, MissedRunPolicyNone, MissedRunPolicyCatchUp:
	default:
		log.Errorf(ErrInvalidMissedRunPolicy.Error())
		return ErrInvalidMissedRunPolicy
	}
	if j.MaxMissedRuns < 0 {
		log.Errorf(ErrInvalidMaxMissedRuns.Error())
		return ErrInvalidMaxMissedRuns
	}
	return nil
}
