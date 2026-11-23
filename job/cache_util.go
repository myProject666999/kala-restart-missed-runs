package job

// Enable and Disable for jobs.
//
// The v1 Enable only re-armed a job that still had a timer:
//
//	shouldStartWaiting := j.jobTimer != nil && j.Disabled
//
// A job loaded disabled at startup has no timer, so enabling it did nothing and
// it never ran again. The rule below is based on the job's state instead of on
// a leftover timer: a job with a usable schedule is armed (or caught up) after
// enabling regardless of whether a timer survived the restart.

func enable(j *Job, cache JobCache, persist bool) error {
	j.lock.Lock()

	wasDisabled := j.Disabled
	j.Disabled = false
	stale := j.jobTimer
	j.jobTimer = nil
	shouldStart := wasDisabled && j.canRunAgainLocked()
	j.lock.Unlock()

	// Stop outside the job lock; the timer may be mid-callback.
	if stale != nil {
		stale.Stop()
	}

	if persist {
		if err := cache.Set(j); err != nil {
			j.lock.Lock()
			j.Disabled = true
			j.lock.Unlock()
			return err
		}
	}

	if shouldStart {
		go j.resumeAfterEnable(cache)
	}

	return nil
}

// resumeAfterEnable treats re-enabling like a small restart: points missed
// while disabled are subject to the same policy as a downtime, then normal
// scheduling resumes.
func (j *Job) resumeAfterEnable(cache JobCache) {
	j.lock.Lock()
	if j.Disabled || j.IsDone {
		j.lock.Unlock()
		return
	}
	now := j.clk.Time().Now()
	policy, cap := j.resolvedMissedRunPolicyLocked()
	missed := cap > 0 && j.hasMissedPointsLocked(now)
	if !missed && policy == MissedRunPolicyNone && !now.Before(j.scheduleTime) {
		// "Do not catch up" means wait for the next whole point, not fire
		// immediately the way the legacy negative wait would.
		j.Metadata.NextOnGrid = true
	}
	j.lock.Unlock()

	if missed {
		j.runCatchUpEpisode(cache)
		return
	}
	j.startWaiting(cache, false, false)
}

// canRunAgainLocked reports whether enabling should schedule the job. The caller
// must hold j.lock.
func (j *Job) canRunAgainLocked() bool {
	if j.IsDone {
		return false
	}
	if j.Schedule != "" {
		return true
	}
	// A one-off job with no schedule and no completion evidence is resumed on
	// enable just as it is at load time.
	return len(j.ParentJobs) == 0 && !j.hasRunEvidenceLocked()
}

func disable(j *Job, cache JobCache, persist bool) error {
	j.lock.Lock()

	j.Disabled = true
	// Keep the (stopped) timer reference: callers such as the API still
	// expect to be able to inspect it, and Enable replaces it explicitly.
	timer := j.jobTimer
	j.lock.Unlock()

	if persist {
		if err := cache.Set(j); err != nil {
			j.lock.Lock()
			j.Disabled = false
			j.lock.Unlock()
			return err
		}
	}

	if timer != nil {
		timer.Stop()
	}

	return nil
}
