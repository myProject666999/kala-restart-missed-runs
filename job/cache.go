package job

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cornelk/hashmap"
	log "github.com/sirupsen/logrus"
)

var (
	ErrJobDoesntExist = errors.New("The job you requested does not exist")
)

type JobCache interface {
	Get(id string) (*Job, error)
	GetAll() *JobsMap
	Set(j *Job) error
	Delete(id string) error
	Persist() error
	Enable(j *Job) error
	Disable(j *Job) error
}

// LoadReport summarizes what Start did with the jobs read from storage.
type LoadReport struct {
	// Loaded is the number of valid jobs added to the cache.
	Loaded int
	// Timers is the number of jobs for which normal scheduling was armed.
	Timers int
	// Skipped is the number of jobs cached without being started (disabled,
	// dependency-only, already finished, or policy "none" with no future
	// point).
	Skipped int
	// Failed is the number of individual stored entries that could not be
	// loaded. It is meaningful only when ReadError is nil.
	Failed int
	// ReadError is set when the whole backend read failed. In that case
	// nothing was loaded and Failed must not be read as "zero bad jobs".
	ReadError error
	// Compensated is the number of catch-up runs recovery planned.
	Compensated int
}

// LoadStatsProvider is implemented by caches that keep the report of the last
// Start call. It is optional so custom caches keep working.
type LoadStatsProvider interface {
	LoadReport() LoadReport
}

type loadReportState struct {
	mu     sync.RWMutex
	report LoadReport
}

func (s *loadReportState) setLoadReport(r LoadReport) {
	s.mu.Lock()
	s.report = r
	s.mu.Unlock()
}

func (s *loadReportState) loadReport() LoadReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.report
}

// JobLoadFailure describes one stored job that could not be loaded.
type JobLoadFailure struct {
	// Id is the stored job id when it is known.
	Id string
	// Err is the decoding or validation error.
	Err error
}

// JobsPartiallyLoadedError is returned by a JobDB when some entries could be
// decoded and others could not. Jobs holds the valid jobs; Failures lists the
// bad ones. Start loads Jobs normally and counts Failures, instead of aborting
// or pretending nothing was wrong.
type JobsPartiallyLoadedError struct {
	Jobs     []*Job
	Failures []JobLoadFailure
}

func (e *JobsPartiallyLoadedError) Error() string {
	return fmt.Sprintf("failed to load %d stored job(s); the remaining jobs were loaded", len(e.Failures))
}

type JobsMap struct {
	Jobs map[string]*Job
	Lock sync.RWMutex
}

func NewJobsMap() *JobsMap {
	return &JobsMap{
		Jobs: map[string]*Job{},
		Lock: sync.RWMutex{},
	}
}

type MemoryJobCache struct {
	// Jobs is a map from Job id's to pointers to the jobs.
	// Used as the main "data store" within this cache implementation.
	jobs           *JobsMap
	jobDB          JobDB
	PersistOnWrite bool
	loadReportState
}

func NewMemoryJobCache(jobDB JobDB) *MemoryJobCache {
	return &MemoryJobCache{
		jobs:  NewJobsMap(),
		jobDB: jobDB,
	}
}

// LoadReport returns the counts from the last Start call.
func (c *MemoryJobCache) LoadReport() LoadReport {
	return c.loadReport()
}

func (c *MemoryJobCache) Start(persistWaitTime time.Duration) {
	if persistWaitTime == 0 {
		c.PersistOnWrite = true
	}

	// Prep cache
	c.loadReportState.setLoadReport(loadJobs(c, c.jobDB))

	// Occasionally, save items in cache to db.
	if persistWaitTime > 0 {
		go c.PersistEvery(persistWaitTime)
	}

	// Process-level defer for shutting down the db.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		s := <-ch
		log.Infof("Process got signal: %s", s)
		log.Infof("Shutting down....")

		// Persist all jobs to database
		if err := c.Persist(); err != nil {
			log.Errorln(err)
		}

		// Close the database
		c.jobDB.Close()

		os.Exit(0)
	}()
}

// loadJobs reads every job from the database and decides, per job, whether to
// cache it, arm its schedule, or leave it for its parent. It never aborts the
// whole cache because one stored entry is bad; see JobsPartiallyLoadedError.
func loadJobs(cache JobCache, jobDB JobDB) LoadReport {
	report := LoadReport{}

	allJobs, err := jobDB.GetAll()
	var partial *JobsPartiallyLoadedError
	if err != nil {
		if !errors.As(err, &partial) {
			// A whole-backend failure is real: report it and load nothing.
			log.Errorf("Error loading jobs from database: %s", err)
			report.ReadError = err
			return report
		}
		for _, f := range partial.Failures {
			log.Errorf("Skipping unloadable stored job %q: %v", f.Id, f.Err)
		}
		report.Failed = len(partial.Failures)
	}

	for _, j := range allJobs {
		if j == nil {
			report.Failed++
			continue
		}
		if j.Id == "" {
			log.Errorf("Skipping stored job with no id")
			report.Failed++
			continue
		}
		if err := j.InitDelayDuration(false); err != nil {
			log.Errorf("Skipping unloadable stored job %q: %v", j.Id, err)
			report.Failed++
			continue
		}
		if err := j.validateMissedRunPolicy(); err != nil {
			log.Errorf("Skipping stored job %q with invalid policy: %v", j.Id, err)
			report.Failed++
			continue
		}

		started, compensated := resumeLoadedJob(cache, j)
		report.Loaded++
		report.Compensated += compensated
		if started {
			report.Timers++
		} else {
			report.Skipped++
		}
	}

	log.Infof(
		"Loaded %d job(s): %d scheduled, %d skipped, %d failed, %d catch-up run(s) planned",
		report.Loaded, report.Timers, report.Skipped, report.Failed, report.Compensated,
	)
	return report
}

// resumeLoadedJob caches one stored job and decides how its schedule resumes.
// The job is always cached so it stays visible through the API; only the
// scheduling differs.
func resumeLoadedJob(cache JobCache, j *Job) (started bool, compensated int) {
	if err := cache.Set(j); err != nil {
		log.Errorf("Error adding job %s:%s to cache: %v", j.Name, j.Id, err)
	}

	// A job read from storage has no clock yet. Use the cache's clock so
	// recovery and scheduling compute against the same "now" as everything
	// else (tests rely on this; production gets the real clock).
	if clker, ok := cache.(Clocker); ok && clker.TimeSet() {
		j.clk.SetClock(clker.Time())
	}

	started, compensated, changed := resumeDecision(cache, j)
	if changed {
		// Persist dropped points / the finished flag so a later restart does
		// not reconsider them. Done outside the job lock.
		j.persist(cache)
	}
	return started, compensated
}

// resumeDecision makes the scheduling decision under the job lock. changed
// reports whether persisted state (the finished flag or the schedule
// watermark) was modified and should be written back.
func resumeDecision(cache JobCache, j *Job) (started bool, compensated int, changed bool) {
	j.lock.Lock()
	defer j.lock.Unlock()

	switch {
	case j.Disabled:
		// Load and keep visible, but do not schedule. Enable will arm it.
		log.Infof("Job %s:%s loaded disabled; not scheduled.", j.Name, j.Id)
		return false, 0, false
	case j.IsDone:
		log.Infof("Job %s:%s already finished; not rescheduled.", j.Name, j.Id)
		return false, 0, false
	}

	if j.Schedule == "" {
		if len(j.ParentJobs) > 0 {
			// A child without its own schedule waits for the dependency and
			// never runs on its own.
			log.Infof("Job %s:%s loaded with parent(s); waiting for dependency.", j.Name, j.Id)
			return false, 0, false
		}
		if j.hasRunEvidenceLocked() {
			// A one-off job that has any record of running is not run
			// again. Without a schedule there is no point to recover, and
			// re-running could repeat work.
			log.Infof("Job %s:%s has no schedule and appears to have run; not re-run.", j.Name, j.Id)
			return false, 0, false
		}
		// A one-off with no completion evidence may have been interrupted
		// before its result was stored; run it once.
		go j.run(cache, TriggerNormal, time.Time{}, false)
		return true, 0, false
	}

	// A job with its own schedule is scheduled even if it also has parents:
	// the schedule trigger and the dependency trigger are independent. Arming
	// here never creates a duplicate for the same point (see startWaiting's
	// de-duplication) and never consumes a persisted point twice.

	// Scheduled job.
	if j.hasFixedRepetitions() && j.remainingPlanRunsLocked() <= 0 {
		j.IsDone = true
		log.Infof("Job %s:%s completed its repetitions; not rescheduled.", j.Name, j.Id)
		return false, 0, true
	}

	now := j.clk.Time().Now()
	policy, cap := j.resolvedMissedRunPolicyLocked()
	if cap > 0 && j.hasMissedPointsLocked(now) {
		plan := j.planRecoveryLocked(now)
		if plan.planned > 0 {
			go j.runCatchUpEpisode(cache)
			return true, plan.planned, false
		}
		// The policy or an already-spent budget allows nothing. Consume
		// the outstanding points anyway: they are dropped, not deferred,
		// so a later restart does not reconsider them, and resume on the
		// grid.
		j.skipOutstandingPointsLocked(now)
		j.Metadata.NextOnGrid = true
		go j.startWaiting(cache, false, false)
		return true, 0, true
	}
	if policy == MissedRunPolicyNone && !now.Before(j.scheduleTime) {
		// "Do not catch up" still means "wait for the next whole point",
		// not "run immediately once" (the legacy negative-wait behavior).
		j.Metadata.NextOnGrid = true
	}

	go j.startWaiting(cache, false, false)
	return true, 0, false
}

// skipOutstandingPointsLocked moves the watermark past every point at or
// before now, so points the recovery budget discarded are not recovered later.
// The caller must hold j.lock.
func (j *Job) skipOutstandingPointsLocked(now time.Time) {
	if lastDue, ok := j.lastGridPointAtOrBeforeLocked(now); ok &&
		lastDue.After(j.LastScheduledRunPoint) {
		j.LastScheduledRunPoint = lastDue
	}
}

func (c *MemoryJobCache) Get(id string) (*Job, error) {
	c.jobs.Lock.RLock()
	defer c.jobs.Lock.RUnlock()

	j := c.jobs.Jobs[id]
	if j == nil {
		return nil, ErrJobDoesntExist
	}

	return j, nil
}

func (c *MemoryJobCache) GetAll() *JobsMap {
	return c.jobs
}

func (c *MemoryJobCache) Set(j *Job) error {
	c.jobs.Lock.Lock()
	defer c.jobs.Lock.Unlock()
	if j == nil {
		return nil
	}

	if c.PersistOnWrite {
		if err := c.jobDB.Save(j); err != nil {
			return err
		}
	}

	c.jobs.Jobs[j.Id] = j
	return nil
}

func (c *MemoryJobCache) Delete(id string) error {
	// Do not hold the cache write lock while acquiring the job lock: a
	// running job holds the job read lock across cache.Get, so taking the
	// locks in the opposite order deadlocks. LockFreeJobCache already looks
	// the job up first; do the same here.
	j, err := c.Get(id)
	if err != nil {
		return ErrJobDoesntExist
	}
	j.lock.Lock()
	defer j.lock.Unlock()

	err = c.jobDB.Delete(id)
	if err != nil {
		err = fmt.Errorf("Error occurred while trying to delete job from db: %s", err)
		if c.PersistOnWrite {
			return err
		}
	}

	j.lock.Unlock()
	j.StopTimer()
	j.lock.Lock()

	go func() {
		log.Errorln(j.DeleteFromParentJobs(c)) // todo: review
	}()

	// Remove itself from dependent jobs as a parent job
	// and possibly delete child jobs if they don't have any other parents.
	go func() {
		log.Errorln(j.DeleteFromDependentJobs(c)) // todo: review
	}()

	c.jobs.Lock.Lock()
	delete(c.jobs.Jobs, id)
	c.jobs.Lock.Unlock()

	return err
}

func (c *MemoryJobCache) Enable(j *Job) error {
	return enable(j, c, c.PersistOnWrite)
}

// Disable stops a job from running by stopping its jobTimer. It also sets Job.Disabled to true,
// which is reflected in the UI.
func (c *MemoryJobCache) Disable(j *Job) error {
	return disable(j, c, c.PersistOnWrite)
}

func (c *MemoryJobCache) Persist() error {
	c.jobs.Lock.RLock()
	defer c.jobs.Lock.RUnlock()
	for _, j := range c.jobs.Jobs {
		err := c.jobDB.Save(j)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *MemoryJobCache) PersistEvery(persistWaitTime time.Duration) {
	wait := time.NewTicker(persistWaitTime).C
	var err error
	for {
		<-wait
		err = c.Persist()
		if err != nil {
			log.Errorf("Error occurred persisting the database. Err: %s", err)
		}
	}
}

type LockFreeJobCache struct {
	jobs            *hashmap.HashMap
	jobDB           JobDB
	retentionPeriod time.Duration
	PersistOnWrite  bool
	Clock
	loadReportState
}

func NewLockFreeJobCache(jobDB JobDB) *LockFreeJobCache {
	return &LockFreeJobCache{
		jobs:            hashmap.New(8), //nolint:gomnd
		jobDB:           jobDB,
		retentionPeriod: -1,
	}
}

// LoadReport returns the counts from the last Start call.
func (c *LockFreeJobCache) LoadReport() LoadReport {
	return c.loadReport()
}

func (c *LockFreeJobCache) Start(persistWaitTime time.Duration, jobstatTtl time.Duration) {
	if persistWaitTime == 0 {
		c.PersistOnWrite = true
	}

	// Prep cache. All backends share one loader, so a job that is valid in
	// one cache is treated the same in the other.
	c.loadReportState.setLoadReport(loadJobs(c, c.jobDB))

	// Occasionally, save items in cache to db.
	if persistWaitTime > 0 {
		go c.PersistEvery(persistWaitTime)
	}

	// Run retention every minute to clean up old job stats entries
	if jobstatTtl > 0 {
		c.retentionPeriod = jobstatTtl
		go c.RetainEvery(1 * time.Minute)
	}

	// Process-level defer for shutting down the db.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		s := <-ch
		log.Infof("Process got signal: %s", s)
		log.Infof("Shutting down....")

		// Persist all jobs to database
		log.Errorln(c.Persist())

		// Close the database
		c.jobDB.Close()

		os.Exit(0)
	}()
}

func (c *LockFreeJobCache) Get(id string) (*Job, error) {
	val, exists := c.jobs.GetStringKey(id)
	if val == nil || !exists {
		return nil, ErrJobDoesntExist
	}
	j := val.(*Job)
	if j == nil {
		return nil, ErrJobDoesntExist
	}
	return j, nil
}

func (c *LockFreeJobCache) GetAll() *JobsMap {
	jm := NewJobsMap()
	for el := range c.jobs.Iter() {
		jm.Jobs[el.Key.(string)] = el.Value.(*Job)
	}
	return jm
}

func (c *LockFreeJobCache) Set(j *Job) error {
	if j == nil {
		return nil
	}

	if c.PersistOnWrite {
		if err := c.jobDB.Save(j); err != nil {
			return err
		}
	}

	c.jobs.Set(j.Id, j)
	return nil
}

func (c *LockFreeJobCache) Delete(id string) error {
	j, err := c.Get(id)
	if err != nil {
		return ErrJobDoesntExist
	}
	j.lock.Lock()
	defer j.lock.Unlock()

	err = c.jobDB.Delete(id)
	if err != nil {
		err = fmt.Errorf("Error occurred while trying to delete job from db: %s", err)
		if c.PersistOnWrite {
			return err
		}
	}

	j.lock.Unlock()
	j.StopTimer()
	j.lock.Lock()

	go func() {
		log.Errorln(j.DeleteFromParentJobs(c)) // todo: review
	}()
	// Remove itself from dependent jobs as a parent job
	// and possibly delete child jobs if they don't have any other parents.
	go func() {
		log.Errorln(j.DeleteFromDependentJobs(c)) // todo: review
	}()
	log.Infof("Deleting %s", id)
	c.jobs.Del(id)
	return err
}

func (c *LockFreeJobCache) Enable(j *Job) error {
	return enable(j, c, c.PersistOnWrite)
}

// Disable stops a job from running by stopping its jobTimer. It also sets Job.Disabled to true,
// which is reflected in the UI.
func (c *LockFreeJobCache) Disable(j *Job) error {
	return disable(j, c, c.PersistOnWrite)
}

func (c *LockFreeJobCache) Persist() error {
	jm := c.GetAll()
	for _, j := range jm.Jobs {
		j.lock.RLock()
		err := c.jobDB.Save(j)
		if err != nil {
			j.lock.RUnlock()
			return err
		}
		j.lock.RUnlock()
	}
	return nil
}

func (c *LockFreeJobCache) PersistEvery(persistWaitTime time.Duration) {
	wait := time.NewTicker(persistWaitTime).C
	var err error
	for {
		<-wait
		err = c.Persist()
		if err != nil {
			log.Errorf("Error occurred persisting the database. Err: %s", err)
		}
	}
}

func (c *LockFreeJobCache) locateJobStatsIndexForRetention(stats []*JobStat) (marker int) {
	now := time.Now()
	expiresAt := now.Add(-c.retentionPeriod)
	pos := -1
	for i, el := range stats {
		diff := el.RanAt.Sub(expiresAt)
		if diff < 0 {
			pos = i
		}
	}
	return pos
}

func (c *LockFreeJobCache) Retain() error {
	for el := range c.jobs.Iter() {
		job := el.Value.(*Job)
		c.compactJobStats(job)
	}
	return nil
}

func (c *LockFreeJobCache) compactJobStats(job *Job) {
	job.lock.Lock()
	defer job.lock.Unlock()
	pos := c.locateJobStatsIndexForRetention(job.Stats)
	if pos >= 0 {
		log.Infof("JobStats TTL: removing %d items", pos+1)
		tmp := make([]*JobStat, len(job.Stats)-pos-1)
		copy(tmp, job.Stats[pos+1:])
		job.Stats = tmp
	}
}

func (c *LockFreeJobCache) RetainEvery(retentionWaitTime time.Duration) {
	wait := time.NewTicker(retentionWaitTime).C
	var err error
	for {
		<-wait
		err = c.Retain()
		if err != nil {
			log.Errorf("Error occurred during invoking retention. Err: %s", err)
		}
	}
}
