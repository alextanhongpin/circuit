package circuit

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"sync/atomic"
)

var (
	ErrCircuitBreaker    = errors.New("circuit")
	ErrUnavailable       = fmt.Errorf("%w: unavailable", ErrCircuitBreaker)
	ErrThresholdExceeded = fmt.Errorf("%w: max threshold %d exceeded", ErrCircuitBreaker, math.MaxUint16)
	ErrStateUnknown      = fmt.Errorf("%w: unknown state", ErrCircuitBreaker)
)

type State uint

const (
	// StateClosed is the initial state of the circuit breaker. Once a certain
	// error threshold is reached, the circuit breaker transitions to StateOpen.
	StateClosed State = iota + 1

	// StateOpen is the state when the circuit breaker timeouts. No calls
	// can be made and ErrUnavailable will be returned.
	// After the timeout ends, the circuit breaker transitions to StateHalfOpen.
	StateOpen

	// StateHalfOpen is the state when the circuit breaker is recovering.
	// Any failed calls will transition the circuit breaker back to StateOpen.
	// After n successive calls is made, the circuit breaker transitions back to
	// StateClosed.
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return ""
	}
}

// IsValid returns true if the state is valid.
func (s State) IsValid() bool {
	return s >= StateClosed && s <= StateHalfOpen
}

// IsOpen returns true if the state is open.
func (s State) IsOpen() bool {
	return s == StateOpen
}

// IsClosed returns true if the state is closed.
func (s State) IsClosed() bool {
	return s == StateClosed
}

// IsHalfOpen returns true if the state is half-open.
func (s State) IsHalfOpen() bool {
	return s == StateHalfOpen
}

type Option struct {
	// The number of times the call must be successful to change the state from
	// half-open to closed.
	Success int64

	// The number of times the call must error to change the state from closed to
	// open.
	Failure int64

	// The timeout duration before the next call can be made once the state is
	// open.
	Timeout time.Duration
	Now     func() time.Time
}

func NewOption() *Option {
	return &Option{
		Success: 10,
		Failure: 10,
		Timeout: 10 * time.Second,
		Now:     time.Now,
	}
}

type CircuitBreaker struct {
	success int64
	failure int64
	timeout time.Duration
	now     func() time.Time
	state   *atomic.Uint64
}

// New returns a pointer to CircuitBreaker.
func New(opt *Option) *CircuitBreaker {
	state := new(atomic.Uint64)
	state.Store(encode(StateClosed, time.Time{}))

	if opt == nil {
		opt = NewOption()
	}

	if opt.Success >= math.MaxUint32 {
		panic(fmt.Errorf("%w: success count too high", ErrThresholdExceeded))
	}

	if opt.Failure >= math.MaxUint32 {
		panic(fmt.Errorf("%w: failure count too high", ErrThresholdExceeded))
	}

	return &CircuitBreaker{
		success: opt.Success,
		failure: opt.Failure,
		timeout: opt.Timeout,
		now:     opt.Now,
		state:   state,
	}
}

// State returns the Circuit Breaker's state.
func (c *CircuitBreaker) State() State {
	n := c.state.Load()
	_, s, _ := decode(n)

	return State(s)
}

// Counter returns the current counter of the circuit breaker.
func (c *CircuitBreaker) Counter() int64 {
	n := c.state.Load()
	counter, _, _ := decode(n)

	return counter
}

// AllowAt returns the next available call time.
func (c *CircuitBreaker) AllowAt() time.Time {
	n := c.state.Load()
	_, _, deadline := decode(n)

	return deadline
}

// Allow returns true if call can be made.
func (c *CircuitBreaker) Allow() bool {
	if c.State().IsOpen() {
		c.Update(false)
	}

	return !c.State().IsOpen()
}

// Update updates the status of the Circuit Breaker.
func (c *CircuitBreaker) Update(ok bool) {
	n := c.state.Load()
	_, state, deadline := decode(n)

	switch State(state) {
	case StateOpen:
		if c.now().After(deadline) {
			c.state.CompareAndSwap(n, encode(StateHalfOpen, time.Time{}))
		}

	case StateHalfOpen:
		if !ok {
			c.state.CompareAndSwap(n, encode(StateOpen, c.nextDeadline()))

			return
		}

		n := c.state.Add(1)
		counter, _, _ := decode(n)
		if int64(counter) > c.success {
			c.state.CompareAndSwap(n, encode(StateClosed, deadline))
		}

	case StateClosed:
		if ok {
			return
		}

		n := c.state.Add(1)
		counter, _, _ := decode(n)
		if int64(counter) > c.failure {
			c.state.CompareAndSwap(n, encode(StateOpen, c.nextDeadline()))
		}
	}
}

// Exec executes and updates the circuit breaker state.
func (c *CircuitBreaker) Exec(fn func() error) error {
	if !c.Allow() {
		return ErrUnavailable
	}

	if err := fn(); err != nil {
		c.Update(false)
		return err
	}

	c.Update(true)

	return nil
}

// nextDeadline returns the next allowed time for the circuit breaker to fire.
// NOTE: Because we are using unix timestamp, the smallest duration is 1s.
// However, just adding a timeout of 1s will lead to incorrect result,
// because the current milliseconds truncated.
// If the current time now is 1.5s, then the next invocation would be 2s,
// which is just 500ms apart.
// We add 0.5s to make fire at time 3s instead.
func (c *CircuitBreaker) nextDeadline() time.Time {
	return c.now().Add(500*time.Millisecond + c.timeout)
}

func Query[T any](cb interface{ Exec(func() error) error }, fn func() (T, error)) (t T, err error) {
	err = cb.Exec(func() error {
		t, err = fn()
		if err != nil {
			return err
		}

		return nil
	})

	return
}

func decode(n uint64) (counter int64, state State, deadline time.Time) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, n)

	c := binary.LittleEndian.Uint16(b)
	s := binary.LittleEndian.Uint16(b[2:])
	d := binary.LittleEndian.Uint32(b[4:])

	counter = int64(c)
	state = State(s)
	deadline = time.Unix(int64(d), 0)

	return
}

// encode encodes the state into a single uint64 atomic state.
//
// atomic state (8 bytes) = counter (2 bytes) + state (2 bytes) + unix timestamp (4 bytes)
//
// The first 2 bytes is the uint16 counter.
// The next 2 bytes is the uint16 state (open, half-open, closed).
// The last 4 bytes is the uint32 unix timestamp representing the deadline.
//
// The state only uses 8 bytes, but we don't have the method to read uint8,
// so the smallest option, uint16 is used instead.
func encode(state State, deadline time.Time) uint64 {
	b := make([]byte, 8)

	binary.LittleEndian.PutUint16(b, 0)
	binary.LittleEndian.PutUint16(b[2:], uint16(state))
	binary.LittleEndian.PutUint32(b[4:], uint32(deadline.Unix()))

	// After writing all the parts, we convert the 8 bytes into uint64.
	return binary.LittleEndian.Uint64(b)
}
