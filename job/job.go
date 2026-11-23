package job

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ajvb/kala/utils/iso8601"
	"github.com/mixer/clock"

	uuid "github.com/nu7hatch/gouuid"
	log "github.com/sirupsen/logrus"
)

const BASE_10 = 10

var (
	RFC3339WithoutTimezone = "2006-01-02T15:04:05"

	ErrInvalidJob             = errors.New("Invalid Local Job. Job's must contain a Name and a Command field")
	ErrInvalidRemoteJob       = errors.New("Invalid Remote Job. Job's must contain a Name and a url field")
	ErrInvalidJobType         = errors.New("Invalid Job type. Types supported: 0 for local and 1 for remote")
	ErrInvalidMissedRunPolicy = errors.New("Invalid missed_run_policy. Supported: \"\" (legacy), \"none\", \"catch_up\"")
	ErrInvalidMaxMissedRuns   = errors.New("Invalid max_missed_runs. It must not be negative")
)

// MissedRunPolicy decides what happens to schedule points that were missed
// while Kala was not running (restart) or while the job was disabled.
type MissedRunPolicy string

const (
	// MissedRunPolicyUnset keeps the legacy behavior: ResumeAtNextScheduledTime
	// decides. When that boolean is false, a single missed run is caught up.
	MissedRunPolicyUnset MissedRunPolicy = ""
	// MissedRunPolicyNone never catches up; the job waits for future points.
	MissedRunPolicyNone MissedRunPolicy = "none"
	// MissedRunPolicyCatchUp catches up missed points, oldest first, up to
	// MaxMissedRuns (default 1).
	MissedRunPolicyCatchUp MissedRunPolicy = "catch_up"
)

// RunTrigger records what caused a run, so normal and compensating runs can
// be told apart in job stats.
type RunTrigger string

const (
	// TriggerNormal is any run without a more specific trigger.
	TriggerNormal RunTrigger = ""
	// TriggerScheduled is a run fired by the job's own schedule timer.
	TriggerScheduled RunTrigger = "scheduled"
	// TriggerCatchUp is a compensating run after downtime.
	TriggerCatchUp RunTrigger = "catch_up"
	// TriggerManual is a run started through the API.
	TriggerManual RunTrigger = "manual"
	// TriggerDependency is a run caused by a parent job finishing.
	TriggerDependency RunTrigger = "dependency"
)

type Job struct {
	Name string `json:"name"`
	Id   string `json:"id"`

	// Command to run
	// e.g. "bash /path/to/my/script.sh"
	Command string `json:"command"`

	// Email of the owner of this job
	// e.g. "admin@example.com"
	Owner string `json:"owner"`

	// Is this job disabled?
	Disabled bool `json:"disabled"`

	// Jobs that are dependent upon this one will be run after this job runs.
	DependentJobs []string `json:"dependent_jobs"`

	// List of ids of jobs that this job is dependent upon.
	ParentJobs []string `json:"parent_jobs"`

	// Job that gets run after all retries have failed consecutively
	OnFailureJob string `json:"on_failure_job"`

	// ISO 8601 String
	// e.g. "R/2014-03-08T20:00:00.000Z/PT2H"
	Schedule     string `json:"schedule"`
	scheduleTime time.Time
	// ISO 8601 Duration struct, used for scheduling
	// job after each run.
	delayDuration *iso8601.Duration

	// Number of times to schedule this job after the
	// first run.
	timesToRepeat int64

	// Number of times to retry on failed attempt for each run.
	Retries uint `json:"retries"`

	// Duration in which it is safe to retry the Job.
	Epsilon         string `json:"epsilon"`
	epsilonDuration *iso8601.Duration

	jobTimer  clock.Timer
	NextRunAt time.Time `json:"next_run_at"`

	// nextScheduledPoint is the schedule point the armed timer is meant to
	// consume. It is an internal detail; it is not persisted.
	nextScheduledPoint time.Time

	// Templating delimiters, the left & right separated by space,
	// for example `{{ }}` or `${ }`.
	//
	// If this field is non-empty, then each time this
	// job is executed, Kala will template its main
	// content as a Go Template with the job itself as data.
	//
	// The Command is templated for local jobs,
	// and Url and Body in RemoteProperties.
	TemplateDelimiters string

	// The clock for this job; used to mock time during tests.
	clk Clock

	// If the job is disabled (or the system inoperative) and we pass
	// the scheduled run point, when the job becomes active again,
	// normally the job will run immediately.
	// With this setting on, it will not run immediately, but will wait
	// until the next scheduled run time comes along.
	//
	// This is the legacy spelling of MissedRunPolicy "none". When
	// MissedRunPolicy is set explicitly it takes precedence.
	ResumeAtNextScheduledTime bool `json:"resume_at_next_scheduled_time"`

	// MissedRunPolicy decides what happens to schedule points that were
	// missed while Kala was down or the job was disabled.
	//
	// The zero value is valid and means "use the legacy behavior", which is
	// to catch up a single missed run and then resume the normal cadence.
	MissedRunPolicy MissedRunPolicy `json:"missed_run_policy,omitempty"`

	// MaxMissedRuns bounds the number of catch-up runs for
	// MissedRunPolicy "catch_up". Values below one behave like one.
	MaxMissedRuns int `json:"max_missed_runs,omitempty"`

	// Meta data about successful and failed runs.
	Metadata Metadata `json:"metadata"`

	// Type of the job
	JobType jobType `json:"type"`

	// Custom properties for the remote job type
	RemoteProperties RemoteProperties `json:"remote_properties"`

	// Collection of Job Stats
	Stats []*JobStat `json:"stats"`

	// LastScheduledRunPoint is the most recent schedule point that the
	// scheduler itself has consumed and completed. It is advanced only by
	// schedule-driven runs (normal timers and catch-up runs), never by
	// manual starts or dependency-triggered runs. That keeps the two
	// trigger sources distinguishable when a job has both a parent and a
	// schedule.
	//
	// It is persisted after each completed run, so a restart resumes from
	// the last completed point instead of recomputing the whole window.
	LastScheduledRunPoint time.Time `json:"last_scheduled_run_point,omitempty"`

	lock sync.RWMutex

	// Says if a job has been executed right numbers of time
	// and should not been executed again in the future
	IsDone bool `json:"is_done"`

	// catchUpInProgress is set while a recovery catch-up sequence is
	// running for this job. While set, completed runs do not arm a normal
	// timer; the catch-up loop decides when normal scheduling resumes.
	catchUpInProgress bool

	// The job will send on this channel when it's done running; used for tests.
	// Note that if the job should be rescheduled, it will send on this channel
	// when it's done rescheduling rather than when the job is done running.
	// That's most useful for testing the scheduling aspect of jobs.
	ranChan chan struct{}

	// Used for testing schedules.
	succeedInstantly bool
}

type jobType int

const (
	LocalJob jobType = iota
	RemoteJob
)

// RemoteProperties Custom properties for the remote job type
type RemoteProperties struct {
	Url    string `json:"url"`
	Method string `json:"method"`

	// A body to attach to the http request
	Body string `json:"body"`

	// A list of headers to add to http request (e.g. [{"key": "charset", "value": "UTF-8"}])
	Headers http.Header `json:"headers"`

	// A timeout property for the http request in seconds
	Timeout int `json:"timeout"`

	// A list of expected response codes (e.g. [200, 201])
	ExpectedResponseCodes []int `json:"expected_response_codes"`
}

type Metadata struct {
	SuccessCount         uint      `json:"success_count"`
	LastSuccess          time.Time `json:"last_success"`
	ErrorCount           uint      `json:"error_count"`
	LastError            time.Time `json:"last_error"`
	LastAttemptedRun     time.Time `json:"last_attempted_run"`
	NumberOfFinishedRuns uint      `json:"number_of_finished_runs"`

	// ScheduledRuns is the number of completed schedule-driven runs (normal
	// or catch-up). It is counted independently of Stats, so jobstat-ttl
	// truncation cannot make a finite job run again, and it is not increased
	// by manual starts, dependency runs, or retries inside a single run.
	ScheduledRuns uint `json:"scheduled_runs,omitempty"`
	// CatchUpRuns is how many catch-up runs this episode has executed. It is
	// persisted after every run, so an interrupted episode continues where
	// it stopped instead of recomputing the whole outage.
	CatchUpRuns uint `json:"catch_up_runs,omitempty"`
	// CatchUpPlanned is the budget this episode is allowed: the number of
	// catch-up runs planned when the episode started. Zero means no episode
	// is in progress.
	CatchUpPlanned uint `json:"catch_up_planned,omitempty"`
	// NextOnGrid asks the next normal timer to snap to the schedule grid
	// (the first point strictly after now) instead of the legacy
	// "last attempt + interval" cadence. It is set when a recovery episode
	// or a skipped window ends, and cleared once the timer is armed.
	NextOnGrid bool `json:"next_on_grid,omitempty"`
	// LastCatchUpResult is the outcome of the most recent catch-up
	// sequence: how many runs were due, how many were executed, and
	// whether the cap truncated them. It is replaced, never appended.
	LastCatchUpResult *CatchUpResult `json:"last_catch_up_result,omitempty"`
}

// CatchUpResult records the outcome of the most recent catch-up sequence.
type CatchUpResult struct {
	// StartedAt is when the recovery pass began.
	StartedAt time.Time `json:"started_at"`
	// MissedPoints is how many schedule points had passed when recovery
	// began, without the cap applied.
	MissedPoints int `json:"missed_points"`
	// Planned is how many catch-up runs the policy and cap allowed.
	Planned int `json:"planned"`
	// Executed is how many catch-up runs actually completed.
	Executed int `json:"executed"`
	// Truncated is true when Planned < MissedPoints, i.e. the cap (or the
	// policy) discarded missed points.
	Truncated bool `json:"truncated"`
	// NextScheduledPoint is the next normal schedule point after recovery.
	NextScheduledPoint time.Time `json:"next_scheduled_point"`
}

// Bytes returns the byte representation of the Job.
func (j Job) Bytes() ([]byte, error) { //nolint:govet // Copying the lock is okay here
	buff := new(bytes.Buffer)
	enc := gob.NewEncoder(buff)
	err := enc.Encode(j) //nolint:govet // Copying the lock is okay here
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

// NewFromBytes returns a Job instance from a byte representation.
func NewFromBytes(b []byte) (*Job, error) {
	j := &Job{}

	buf := bytes.NewBuffer(b)
	err := gob.NewDecoder(buf).Decode(j)
	if err != nil {
		return nil, err
	}

	return j, nil
}

// Init fills in the protected fields and parses the iso8601 notation.
// It also adds the job to the Cache
func (j *Job) Init(cache JobCache) error {
	j.lock.Lock()
	defer j.lock.Unlock()

	// use the cache's clock if available (useful for tests)
	if clker, ok := cache.(Clocker); ok {
		if clker.TimeSet() {
			j.clk.SetClock(clker.Time())
		}
	}

	// validate job type and params
	err := j.validation()
	if err != nil {
		return err
	}
	if err := j.validateMissedRunPolicy(); err != nil {
		return err
	}

	u4, err := uuid.NewV4()
	if err != nil {
		log.Errorf("Error occurred when generating uuid: %s", err)
		return err
	}
	j.Id = u4.String()

	// Verify the job's parents before adding it to the cache. Otherwise a
	// missing parent leaves a child that the API reported as rejected.
	parentJobs := make([]*Job, 0, len(j.ParentJobs))
	for _, p := range j.ParentJobs {
		parentJob, err := cache.Get(p)
		if err != nil {
			return err
		}
		parentJobs = append(parentJobs, parentJob)
	}

	// Add Job to the cache.
	j.lock.Unlock()
	err = cache.Set(j)
	j.lock.Lock()
	if err != nil {
		return err
	}

	if len(parentJobs) != 0 {
		// Add new job to parent jobs
		for _, parentJob := range parentJobs {
			parentJob.DependentJobs = append(parentJob.DependentJobs, j.Id)
		}

		return nil
	}

	// TODO: Delete from cache after running.
	if j.Schedule == "" {
		// If schedule is empty, its a one-off job.
		go j.run(cache, TriggerNormal, time.Time{}, false)
		return nil
	}

	j.lock.Unlock()
	err = j.InitDelayDuration(true)
	j.lock.Lock()
	if err != nil {
		j.lock.Unlock()
		_ = cache.Delete(j.Id)
		j.lock.Lock()
		return err
	}

	j.lock.Unlock()
	j.StartWaiting(cache, false)

	j.lock.Lock()

	return nil
}

// InitDelayDuration is used to parsed the iso8601 Schedule notation into its relevant fields in the Job struct.
// If checkTime is true, then it will return an error if the Scheduled time has passed.
func (j *Job) InitDelayDuration(checkTime bool) error {
	j.lock.Lock()
	defer j.lock.Unlock()

	if j.Schedule == "" {
		return nil
	}

	var err error
	splitTime := strings.Split(j.Schedule, "/")
	if len(splitTime) != 3 { //nolint:gomnd
		return fmt.Errorf(
			"Schedule not formatted correctly. Should look like: R/2014-03-08T20:00:00Z/PT2H",
		)
	}

	// Handle Repeat Amount
	if splitTime[0] == "R" {
		// Repeat forever
		j.timesToRepeat = -1
	} else {
		// A repeat part with no "R" in it (for example "X") has no second element
		// after splitting, so check before indexing.
		repeatParts := strings.Split(splitTime[0], "R")
		if len(repeatParts) < 2 { //nolint:gomnd
			return fmt.Errorf(
				"Schedule repeat part %q must be R or R<number>. Should look like: R/2014-03-08T20:00:00Z/PT2H",
				splitTime[0],
			)
		}
		j.timesToRepeat, err = strconv.ParseInt(repeatParts[1], BASE_10, 0)
		if err != nil {
			log.Errorf("Error converting timesToRepeat to an int: %s", err)
			return err
		}
	}

	j.scheduleTime, err = time.Parse(time.RFC3339, splitTime[1])
	if err != nil {
		j.scheduleTime, err = time.Parse(RFC3339WithoutTimezone, splitTime[1])
		if err != nil {
			log.Errorf("Error converting scheduleTime to a time.Time: %s", err)
			return err
		}
	}
	if checkTime {
		diff := j.scheduleTime.Sub(j.clk.Time().Now())
		if diff < 0 {
			return fmt.Errorf("Job %s:%s cannot be scheduled %s ago", j.Name, j.Id, diff.String())
		}
	}
	log.Debugf("Job %s:%s scheduled", j.Name, j.Id)
	log.Debugf("Starting %s will repeat for %d", j.scheduleTime, j.timesToRepeat)

	if j.timesToRepeat != 0 {
		j.delayDuration, err = iso8601.FromString(splitTime[2])
		if err != nil {
			log.Errorf("Error converting delayDuration to a iso8601.Duration: %s", err)
			return err
		}
		log.Debugf("Delay duration is %s", j.delayDuration.RelativeTo(j.clk.Time().Now()))
	}

	if j.Epsilon != "" {
		j.epsilonDuration, err = iso8601.FromString(j.Epsilon)
		if err != nil {
			log.Errorf("Error converting j.Epsilon to iso8601.Duration: %s", err)
			return err
		}
	}
	return nil
}

// StartWaiting begins a timer for when it should execute the Jobs .Run() method.
func (j *Job) StartWaiting(cache JobCache, justRan bool) {
	j.startWaiting(cache, justRan, false)
}

// startWaiting arms the next normal schedule timer. It never compensates
// missed runs; callers that need compensation call runCatchUpEpisode.
//
// It is idempotent for the same plan point: arming the same (job, point)
// twice is refused, which prevents two timers for one persistent point.
func (j *Job) startWaiting(cache JobCache, justRan, force bool) {
	j.lock.Lock()

	if j.Disabled {
		j.lock.Unlock()
		return
	}
	now := j.clk.Time().Now()
	alignToGrid := j.Metadata.NextOnGrid
	point, waitDuration, ok := j.normalWaitLocked(now, alignToGrid)
	if !ok {
		j.lock.Unlock()
		if justRan && j.ranChan != nil {
			j.ranChan <- struct{}{}
		}
		return
	}

	if !force && j.jobTimer != nil && j.NextRunAt.After(now) &&
		!j.nextScheduledPoint.IsZero() && j.nextScheduledPoint.Equal(point) {
		// A timer for this exact point is already pending; do not create a
		// second one for the same persistent point.
		j.lock.Unlock()
		return
	}

	log.Infof("Job %s:%s repeating in %s", j.Name, j.Id, waitDuration)

	j.NextRunAt = now.Add(waitDuration)
	j.nextScheduledPoint = point
	j.Metadata.NextOnGrid = false

	// Replace any previous timer so a job never has two live timers.
	stale := j.jobTimer
	jobRun := func() { j.runScheduled(cache, point) }
	j.jobTimer = j.clk.Time().AfterFunc(waitDuration, jobRun)
	ran := justRan && j.ranChan != nil
	j.lock.Unlock()

	if stale != nil {
		stale.Stop()
	}
	if ran {
		j.ranChan <- struct{}{}
	}
}

func (j *Job) GetWaitDuration() time.Duration {
	j.lock.RLock()
	defer j.lock.RUnlock()

	waitDuration := time.Duration(j.scheduleTime.UnixNano() - j.clk.Time().Now().UnixNano())

	if waitDuration >= 0 {
		return waitDuration
	}

	if j.timesToRepeat == 0 {
		return 0
	}

	if j.ResumeAtNextScheduledTime {

		// In cases where the scheduled point is very long ago,
		// and the delayDuration interval is very small,
		// it can take an extremely long time to compute the next scheduled run time.
		// One potential case (programmer error) is having the scheduleTime be zero.
		// (This would be due to not calling InitDelayDuration on a job.)
		// For now, we spot this and handle it as a special case.
		if j.scheduleTime.IsZero() {
			return 0
		}

		newRunPoint := j.scheduleTime
		for newRunPoint.Before(j.clk.Time().Now()) {
			newRunPoint = j.delayDuration.Add(newRunPoint)
		}

		return newRunPoint.Sub(j.clk.Time().Now())
	}

	if j.Metadata.LastAttemptedRun.IsZero() {
		waitDuration = j.delayDuration.RelativeTo(j.clk.Time().Now())
	} else {
		lastRun := j.Metadata.LastAttemptedRun
		// Needs to be recalculated each time because of Months.
		lastRun = j.delayDuration.Add(lastRun)
		waitDuration = lastRun.Sub(j.clk.Time().Now())
	}

	return waitDuration
}

// Disable stops the job from running by stopping its jobTimer. It also sets Job.Disabled to true,
// which is reflected in the UI.
func (j *Job) Disable(cache JobCache) error {
	return cache.Disable(j)
}

func (j *Job) Enable(cache JobCache) error {
	return cache.Enable(j)
}

// DeleteFromParentJobs goes through and deletes the current job from any parent jobs.
func (j *Job) DeleteFromParentJobs(cache JobCache) error {
	j.lock.Lock()
	defer j.lock.Unlock()

	for _, p := range j.ParentJobs {
		parentJob, err := cache.Get(p)

		if err != nil {
			return err
		}

		parentJob.lock.Lock()

		ndx := 0
		for i, id := range parentJob.DependentJobs {
			if id == j.Id {
				ndx = i
				break
			}
		}
		parentJob.DependentJobs = append(
			parentJob.DependentJobs[:ndx], parentJob.DependentJobs[ndx+1:]...,
		)
		err = cache.Set(parentJob)
		if err != nil {
			return err
		}
		parentJob.lock.Unlock()
	}

	return nil
}

// DeleteFromDependentJobs
func (j *Job) DeleteFromDependentJobs(cache JobCache) error {
	j.lock.RLock()
	defer j.lock.RUnlock()

	for _, id := range j.DependentJobs {
		childJob, err := cache.Get(id)
		if err != nil {
			return err
		}

		// If there are no other parent jobs, delete this job.
		if len(childJob.ParentJobs) == 1 {
			log.Infof("Deleting child %s", id)
			_ = cache.Delete(childJob.Id)
			continue
		}

		childJob.lock.Lock()

		ndx := 0
		for i, id := range childJob.ParentJobs {
			if id == j.Id {
				ndx = i
				break
			}
		}
		childJob.ParentJobs = append(
			childJob.ParentJobs[:ndx], childJob.ParentJobs[ndx+1:]...,
		)

		childJob.lock.Unlock()

	}

	return nil
}

// Runs the on failure job, if it exists. Does not lock the parent job - it is up to you to do this
// however you want
func (j *Job) RunOnFailureJob(cache JobCache) {
	if j.OnFailureJob != "" {
		onFailureJob, cacheErr := cache.Get(j.OnFailureJob)
		if cacheErr == ErrJobDoesntExist {
			log.Errorf("Error retrieving dependent job with id of %s", j.OnFailureJob)
		} else {
			onFailureJob.run(cache, TriggerDependency, time.Time{}, false)
		}
	}
}

// Run executes the job without a schedule point. It is used for manual starts.
func (j *Job) Run(cache JobCache) {
	j.run(cache, TriggerManual, time.Time{}, false)
}

// runScheduled is the timer callback: it consumes the schedule point the
// timer was armed for.
func (j *Job) runScheduled(cache JobCache, point time.Time) {
	j.run(cache, TriggerScheduled, point, true)
}

// run executes the job and reports whether a run actually happened. scheduled
// tells whether this run consumes a schedule point; point is that point (a zero
// value means "the next outstanding point").
func (j *Job) run(cache JobCache, trigger RunTrigger, point time.Time, scheduled bool) bool {
	_, err := cache.Get(j.Id)
	if errors.Is(err, ErrJobDoesntExist) {
		log.Infof("Job %s with id %s tried to run, but exited early because it has been deleted", j.Name, j.Id)
		return false
	}

	j.lock.RLock()
	jobRunner := &JobRunner{job: j, meta: j.Metadata, trigger: trigger, scheduledPoint: point}
	// Capture legacy evidence before the run so seeding the independent
	// counter does not count this run from its own post-run metadata.
	legacyDoneBeforeRun := j.planRunsDoneLocked()
	j.lock.RUnlock()

	newStat, newMeta, err := jobRunner.Run(cache)
	if err != nil {
		j.lock.RLock()
		j.RunOnFailureJob(cache)
		j.lock.RUnlock()
	}
	executed := newStat != nil

	j.lock.Lock()
	j.Metadata = newMeta
	if scheduled && executed {
		// Seed the independent counter from legacy evidence before the first
		// increment; otherwise a job upgraded from old data would forget the
		// runs it already did and could run past its plan.
		if j.Metadata.ScheduledRuns == 0 {
			j.Metadata.ScheduledRuns = uint(legacyDoneBeforeRun)
		}
		j.Metadata.ScheduledRuns++
		if jobRunner.trigger == TriggerCatchUp {
			j.Metadata.CatchUpRuns++
		}
		consumed := point
		if consumed.IsZero() {
			if oldest, ok := j.nextOutstandingPointLocked(j.clk.Time().Now()); ok {
				consumed = oldest
			}
		}
		if !consumed.IsZero() && consumed.After(j.LastScheduledRunPoint) {
			j.LastScheduledRunPoint = consumed
		}
	}
	if newStat != nil {
		j.Stats = append(j.Stats, newStat)
	}

	resuming := j.catchUpInProgress
	j.lock.Unlock()

	if !executed {
		// Disabled or deleted before running: nothing changed, nothing to
		// reschedule.
		return false
	}

	// Persist before arming anything else, so a restart sees the point as
	// consumed. The catch-up loop persists each completed point itself.
	j.persist(cache)

	if scheduled && resuming {
		// The catch-up loop owns scheduling until the episode ends.
		return true
	}

	if j.ShouldStartWaiting() {
		go j.startWaiting(cache, true, false)
		return true
	}

	j.lock.Lock()
	j.IsDone = true
	ran := j.ranChan != nil
	j.lock.Unlock()

	if ran {
		j.ranChan <- struct{}{}
	}
	return true
}

func (j *Job) StopTimer() {
	j.lock.Lock()
	timer := j.jobTimer
	j.jobTimer = nil
	j.lock.Unlock()

	if timer != nil {
		timer.Stop()
	}
}

func (j *Job) RunCmd() (string, error) {
	j.lock.RLock()
	defer j.lock.RUnlock()

	jobRunner := &JobRunner{job: j}
	return jobRunner.runCmd()
}

func (j *Job) hasFixedRepetitions() bool {
	return j.timesToRepeat != -1
}

// ShouldStartWaiting reports whether the job may be scheduled again.
//
// A fixed-repetition job is finished when its independent run counter has
// reached the plan. The counter (Metadata.ScheduledRuns) is not truncated by
// jobstat-ttl, unlike len(Stats).
func (j *Job) ShouldStartWaiting() bool {
	j.lock.RLock()
	defer j.lock.RUnlock()
	return j.shouldStartWaitingLocked()
}

// shouldStartWaitingLocked is ShouldStartWaiting for callers that already hold
// the lock.
func (j *Job) shouldStartWaitingLocked() bool {
	if j.Disabled {
		return false
	}

	if j.hasFixedRepetitions() && j.remainingPlanRunsLocked() <= 0 {
		return false
	}
	return true
}

func (j *Job) validation() error {
	var err error
	switch {
	case j.JobType == LocalJob && (j.Name == "" || j.Command == ""):
		err = ErrInvalidJob
	case j.JobType == RemoteJob && (j.Name == "" || j.RemoteProperties.Url == ""):
		err = ErrInvalidRemoteJob
	case j.JobType != LocalJob && j.JobType != RemoteJob:
		err = ErrInvalidJobType
	default:
		return nil
	}
	log.Errorf(err.Error())
	return err
}

func (j *Job) SetClock(clk clock.Clock) {
	j.clk.SetClock(clk)
}

// Type alias for the recursive call
type RJob Job

// need this to fix race condition
func (j *Job) MarshalJSON() ([]byte, error) {
	j.lock.RLock()
	defer j.lock.RUnlock()
	return json.Marshal(RJob(*j)) //nolint:govet // Copying the lock is okay here
}
