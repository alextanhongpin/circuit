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

type Breaker struct {
	success int64
	failure int64
	timeout time.Duration
	now     func() time.Time
	state   *atomic.Uint64
}

// New returns a pointer to Breaker.
func New(opt *Option) *Breaker {
	state := new(atomic.Uint64)
	state.Store(newAtomicState(StateClosed, time.Time{}).Uint64())

	if opt == nil {
		opt = NewOption()
	}

	if opt.Success >= math.MaxUint16 {
		panic(fmt.Errorf("%w: success count too high", ErrThresholdExceeded))
	}

	if opt.Failure >= math.MaxUint16 {
		panic(fmt.Errorf("%w: failure count too high", ErrThresholdExceeded))
	}

	return &Breaker{
		success: opt.Success,
		failure: opt.Failure,
		timeout: opt.Timeout,
		now:     opt.Now,
		state:   state,
	}
}

// State returns the Circuit Breaker's state.
func (c *Breaker) State() State {
	return c.atomicState().State()
}

// Counter returns the current counter of the circuit breaker.
func (c *Breaker) Counter() int64 {
	return c.atomicState().Counter()
}

// AllowAt returns the next available call time.
func (c *Breaker) AllowAt() time.Time {
	return c.atomicState().Deadline()
}

// Allow returns true if call can be made.
func (c *Breaker) Allow() bool {
	if !c.State().IsOpen() {
		return true
	}

	return c.Update(false)
}

// Update updates the status of the Circuit Breaker.
func (c *Breaker) Update(ok bool) bool {
	s := c.atomicState()

	switch s.State() {
	case StateOpen:
		if c.isDeadlineExceeded(s.Deadline()) {
			return c.toHalfOpen(s.Uint64())
		}

	case StateHalfOpen:
		if !ok {
			return c.toOpen(s.Uint64())
		}

		n := c.state.Add(1)
		if c.isThresholdExceeded(n, c.success) {
			return c.toClosed(n)
		}
	case StateClosed:
		if ok {
			return false
		}

		n := c.state.Add(1)
		if c.isThresholdExceeded(n, c.failure) {
			return c.toOpen(n)
		}
	}

	return false
}

// Exec executes and updates the circuit breaker state.
func (c *Breaker) Exec(fn func() error) error {
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

func (c *Breaker) atomicState() AtomicState {
	return newAtomicStateFromUint64(c.state.Load())
}

func (c *Breaker) isDeadlineExceeded(deadline time.Time) bool {
	return c.now().After(deadline)
}

func (c *Breaker) isThresholdExceeded(state uint64, threshold int64) bool {
	s := newAtomicStateFromUint64(state)
	return s.Counter() > threshold
}

func (c *Breaker) toClosed(state uint64) bool {
	return c.state.CompareAndSwap(state, newAtomicState(StateClosed, time.Time{}).Uint64())
}

func (c *Breaker) toHalfOpen(state uint64) bool {
	return c.state.CompareAndSwap(state, newAtomicState(StateHalfOpen, time.Time{}).Uint64())
}

func (c *Breaker) toOpen(state uint64) bool {
	deadline := c.now().Add(c.timeout)
	return c.state.CompareAndSwap(state, newAtomicState(StateOpen, deadline).Uint64())
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

type AtomicState [8]byte

func newAtomicStateFromUint64(n uint64) AtomicState {
	var b AtomicState
	binary.LittleEndian.PutUint64(b[:], n)
	return b
}

func (as AtomicState) Uint64() uint64 {
	return binary.LittleEndian.Uint64(as[:])
}

func (as AtomicState) Counter() int64 {
	v := binary.LittleEndian.Uint16(as[:])

	return int64(v)
}

func (as AtomicState) State() State {
	v := binary.LittleEndian.Uint16(as[2:])

	return State(v)
}

func (as AtomicState) Deadline() time.Time {
	ms := binary.LittleEndian.Uint16(as[:])
	ts := binary.LittleEndian.Uint32(as[4:])

	return time.Unix(int64(ts), int64(ms)*1e6)
}

// newAtomicState encodes the state into a single uint64 atomic state.
//
// atomic state (8 bytes) = counter (2 bytes) + state (2 bytes) + unix timestamp (4 bytes)
//
// The first 2 bytes is the uint16 counter.
// The next 2 bytes is the uint16 state (open, half-open, closed).
// The last 4 bytes is the uint32 unix timestamp representing the deadline.
//
// The state only uses 8 bytes, but we don't have the method to read uint8,
// so the smallest option, uint16 is used instead.
func newAtomicState(state State, deadline time.Time) AtomicState {
	var b AtomicState

	var counter uint16

	// Deadline is non-zero when the state is `open`.
	// We use the space used to store counter to store the deadline's
	// milliseconds since counter will be 0.
	// The additional millisecond is improves the precision of the timeout.
	if !deadline.IsZero() {
		// NOTE: Always round up to the nearest millisecond. Otherwise, the timeout
		// will always complete earlier.
		ns := deadline.Add(500_000 * time.Nanosecond).Round(1 * time.Millisecond).UnixNano()
		counter = uint16(ns / 1e6 % 1e3)
	}

	binary.LittleEndian.PutUint16(b[:], counter)
	binary.LittleEndian.PutUint16(b[2:], uint16(state))
	binary.LittleEndian.PutUint32(b[4:], uint32(deadline.Unix()))

	return b
}
