package job

import (
	"sync"
	"time"

	// This library abstracts the time functionality of the OS so that it can be controlled during unit tests.
	// It was selected over thejerf/abtime because abtime is geared towards precision timing rather than scheduling.
	// Other libraries were reviewed and rejected due to outstanding bugs.
	"github.com/mixer/clock"
)

type Clock struct {
	clock.Clock
	lock sync.RWMutex
}

func (clk *Clock) SetClock(in clock.Clock) {
	clk.lock.Lock()
	defer clk.lock.Unlock()
	clk.Clock = in
}

func (clk *Clock) Time() clock.Clock {
	clk.lock.RLock()
	defer clk.lock.RUnlock()

	if clk.Clock == nil {
		clk.lock.RUnlock()
		clk.lock.Lock()
		clk.Clock = realClock{}
		clk.lock.Unlock()
		clk.lock.RLock()
	}

	return clk.Clock
}

func (clk *Clock) TimeSet() (result bool) {
	clk.lock.RLock()
	result = clk.Clock != nil
	clk.lock.RUnlock()
	return
}

type Clocker interface {
	Time() clock.Clock
	TimeSet() bool
}

// realClock is the production clock.
//
// It deliberately does not use clock.C from mixer/clock: that implementation
// stores a time.Timer by value (`defaultTimer{*time.AfterFunc(...)}`). Copying a
// time.Timer after it has been used is invalid, and current Go runtimes report
// it as "stop of synctest timer from outside bubble" when the copy is stopped
// from another goroutine (for example an HTTP handler deleting a job). The
// timers here are always held through a pointer.
type realClock struct{}

var _ clock.Clock = realClock{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (realClock) Sleep(d time.Duration)                  { time.Sleep(d) }
func (realClock) Tick(d time.Duration) <-chan time.Time  { return time.Tick(d) } //nolint:staticcheck // matches clock.Clock
func (realClock) Since(t time.Time) time.Duration        { return time.Since(t) }

func (realClock) AfterFunc(d time.Duration, f func()) clock.Timer {
	return &realTimer{time.AfterFunc(d, f)}
}

func (realClock) NewTimer(d time.Duration) clock.Timer {
	return &realTimer{time.NewTimer(d)}
}

func (realClock) NewTicker(d time.Duration) clock.Ticker {
	return &realTicker{time.NewTicker(d)}
}

// realTimer holds the underlying time.Timer through a pointer so it is never
// copied by value.
type realTimer struct {
	t *time.Timer
}

var _ clock.Timer = (*realTimer)(nil)

func (r *realTimer) Chan() <-chan time.Time { return r.t.C }

func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

func (r *realTimer) Stop() bool { return r.t.Stop() }

// realTicker is the ticker counterpart of realTimer.
type realTicker struct {
	t *time.Ticker
}

var _ clock.Ticker = (*realTicker)(nil)

func (r *realTicker) Chan() <-chan time.Time { return r.t.C }

func (r *realTicker) Stop() { r.t.Stop() }
