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
)

type State uint

const (
	StateClosed State = iota + 1
	StateOpen
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

func (s State) IsOpen() bool {
	return s == StateOpen
}

func (s State) IsClosed() bool {
	return s == StateClosed
}

func (s State) IsHalfOpen() bool {
	return s == StateHalfOpen
}

type Option struct {
	Success int64
	Failure int64
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
		c.Update(true)
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
			c.state.CompareAndSwap(n, encode(StateHalfOpen, deadline))
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

func (c *CircuitBreaker) nextDeadline() time.Time {
	// NOTE: Because we are using unix timestamp, the smallest duration is 1s.
	// However, just adding a timeout of 1s will lead to incorrect result,
	// because the current milliseconds truncated.
	// If the current time now is 1.5s, then the next invocation would be 2s,
	// which is just 500ms apart.
	// We add 0.5s to make fire at time 3s instead.
	return c.now().Add(500*time.Millisecond + c.timeout)
}

type circuitBreaker interface {
	Allow() bool
	Update(success bool)
}

func Exec(cb circuitBreaker, fn func() error) error {
	if !cb.Allow() {
		return ErrUnavailable
	}

	if err := fn(); err != nil {
		cb.Update(false)

		return err
	}

	cb.Update(true)

	return nil
}

func Query[T any](cb circuitBreaker, fn func() (T, error)) (T, error) {
	if !cb.Allow() {
		var t T
		return t, ErrUnavailable
	}

	t, err := fn()
	if err != nil {
		cb.Update(false)

		return t, err
	}

	cb.Update(true)

	return t, nil
}

func decode(s uint64) (counter int64, state State, deadline time.Time) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, s)

	counter = int64(binary.LittleEndian.Uint16(b))
	state = State(binary.LittleEndian.Uint16(b[2:]))
	deadline = time.Unix(int64(binary.LittleEndian.Uint32(b[4:])), 0)

	return
}

func encode(state State, deadline time.Time) uint64 {
	// We can represent uint64 using 8 bytes,
	// The first 2 bytes is the uint16 counter.
	// The next 2 bytes is the uint16 state (open, half-open, closed).
	// The last 4 bytes is the uint32 unix timestamp representing the deadline.
	// The state only uses 8 bytes, but we don't have the method to read uint8,
	// so the smallest option, uint16 is used instead.
	b := make([]byte, 8)

	binary.LittleEndian.PutUint16(b, 0)
	binary.LittleEndian.PutUint16(b[2:], uint16(state))
	binary.LittleEndian.PutUint32(b[4:], uint32(deadline.Unix()))

	// After writing all the parts, we convert the 8 bytes into uint64.
	return binary.LittleEndian.Uint64(b)
}
