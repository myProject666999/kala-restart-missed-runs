package job

import (
	"time"
)

// KalaStats is the struct for storing app-level metrics
type KalaStats struct {
	ActiveJobs   int `json:"active_jobs"`
	DisabledJobs int `json:"disabled_jobs"`
	Jobs         int `json:"jobs"`

	ErrorCount   uint `json:"error_count"`
	SuccessCount uint `json:"success_count"`

	NextRunAt        time.Time `json:"next_run_at"`
	LastAttemptedRun time.Time `json:"last_attempted_run"`

	// LoadFailedJobs is how many stored jobs could not be loaded at the last
	// Start. The field is additive: older clients ignore it.
	LoadFailedJobs int `json:"load_failed_jobs"`

	CreatedAt time.Time `json:"created"`
}

// NewKalaStats is used to easily generate a current app-level metrics report.
func NewKalaStats(cache JobCache) *KalaStats {
	ks := &KalaStats{
		CreatedAt: time.Now(),
	}
	if provider, ok := cache.(LoadStatsProvider); ok {
		r := provider.LoadReport()
		// A whole-backend read failure is not surfaced here as a count: it
		// is reported explicitly at Start (log) and must not look like zero
		// bad jobs. Only per-entry failures are counted.
		if r.ReadError == nil && r.Failed > 0 {
			ks.LoadFailedJobs = r.Failed
		}
	}
	jobs := cache.GetAll()
	jobs.Lock.RLock()
	defer jobs.Lock.RUnlock()

	ks.Jobs = len(jobs.Jobs)
	if ks.Jobs == 0 {
		return ks
	}

	nextRun := time.Time{}
	lastRun := time.Time{}
	for _, job := range jobs.Jobs {
		func() {
			job.lock.RLock()
			defer job.lock.RUnlock()

			if job.Disabled {
				ks.DisabledJobs += 1
			} else {
				ks.ActiveJobs += 1
			}

			if nextRun.IsZero() {
				nextRun = job.NextRunAt
			} else if (nextRun.UnixNano() - job.NextRunAt.UnixNano()) > 0 {
				nextRun = job.NextRunAt
			}

			if lastRun.IsZero() {
				if !job.Metadata.LastAttemptedRun.IsZero() {
					lastRun = job.Metadata.LastAttemptedRun
				}
			} else if (lastRun.UnixNano() - job.Metadata.LastAttemptedRun.UnixNano()) < 0 {
				lastRun = job.Metadata.LastAttemptedRun
			}

			ks.ErrorCount += job.Metadata.ErrorCount
			ks.SuccessCount += job.Metadata.SuccessCount
		}()
	}
	ks.NextRunAt = nextRun
	ks.LastAttemptedRun = lastRun

	return ks
}

// JobStat is used to store metrics about a specific Job .Run()
type JobStat struct {
	JobId             string        `json:"job_id"`
	RanAt             time.Time     `json:"ran_at"`
	NumberOfRetries   uint          `json:"number_of_retries"`
	Success           bool          `json:"success"`
	ExecutionDuration time.Duration `json:"execution_duration"`
	// Trigger distinguishes a normal scheduled run from a compensating
	// catch-up run. Empty for legacy entries and for triggers that did not
	// exist before this field.
	Trigger RunTrigger `json:"trigger,omitempty"`
	// ScheduledPoint is the schedule point a schedule-driven run consumed.
	// Zero for runs that were not caused by the schedule.
	ScheduledPoint time.Time `json:"scheduled_point,omitempty"`
}

func NewJobStat(id string) *JobStat {
	return &JobStat{
		JobId: id,
		RanAt: time.Now(),
	}
}
