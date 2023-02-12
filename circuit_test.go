package circuit_test

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/alextanhongpin/circuit"
	"github.com/stretchr/testify/assert"
)

var ErrBadRequest = errors.New("bad request")

func TestState(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		assert := assert.New(t)

		assert.Equal("closed", circuit.StateClosed.String())
		assert.Equal("open", circuit.StateOpen.String())
		assert.Equal("half-open", circuit.StateHalfOpen.String())
		assert.Equal("", circuit.State(99).String())
	})

	t.Run("validity", func(t *testing.T) {
		assert := assert.New(t)

		assert.True(circuit.StateClosed.IsValid())
		assert.True(circuit.StateOpen.IsValid())
		assert.True(circuit.StateHalfOpen.IsValid())
		assert.False(circuit.State(99).IsValid())
	})
}

func TestOption(t *testing.T) {
	t.Run("max success threshold", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("did not panic")
			}
		}()

		opt := &circuit.Option{
			Success: math.MaxUint32 + 1,
		}
		_ = circuit.New(opt)
	})

	t.Run("max failure threshold", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("did not panic")
			}
		}()

		opt := &circuit.Option{
			Failure: math.MaxUint32 + 1,
		}
		_ = circuit.New(opt)
	})
}

func TestCircuit(t *testing.T) {
	assert := assert.New(t)

	opt := &circuit.Option{
		Success: 3,
		Failure: 3,
		Now:     time.Now,
		Timeout: 1 * time.Second,
	}
	cb := circuit.New(opt)

	assert.Equal(circuit.StateClosed, cb.State())

	errHandler := func() error {
		return ErrBadRequest
	}

	okHandler := func() error {
		return nil
	}

	var errWg sync.WaitGroup
	errWg.Add(3)

	assert.Nil(cb.Exec(okHandler))

	for i := 0; i < 3; i++ {
		go func() {
			defer errWg.Done()

			assert.Equal(ErrBadRequest, cb.Exec(errHandler))
			assert.True(cb.State().IsClosed())
		}()
	}
	errWg.Wait()

	assert.Equal(ErrBadRequest, cb.Exec(errHandler))
	assert.True(cb.State().IsOpen())

	assert.Equal(circuit.ErrCircuitOpen, cb.Exec(errHandler))
	assert.True(cb.State().IsOpen())

	sleep := cb.AllowAt().Sub(time.Now())
	time.Sleep(sleep)

	var okWg sync.WaitGroup
	okWg.Add(3)

	for i := 0; i < 3; i++ {
		go func() {
			defer okWg.Done()

			assert.Nil(cb.Exec(okHandler))
			assert.True(cb.State().IsHalfOpen())
		}()
	}
	okWg.Wait()

	assert.Nil(cb.Exec(okHandler))
	assert.True(cb.State().IsClosed())
}

func TestQuery(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		assert := assert.New(t)

		cb := circuit.New(nil)
		assert.Equal(circuit.StateClosed, cb.State())
		assert.Equal(int64(0), cb.Counter())

		handler := func() (string, error) {
			return "hello world", nil
		}

		res, err := circuit.Query(cb, handler)
		assert.Nil(err)
		assert.Equal("hello world", res)
	})

	t.Run("sad path", func(t *testing.T) {
		assert := assert.New(t)

		opt := &circuit.Option{
			Success: 3,
			Failure: 3,
			Now:     time.Now,
			Timeout: 1 * time.Second,
		}

		cb := circuit.New(opt)
		assert.Equal(circuit.StateClosed, cb.State())
		assert.Equal(int64(0), cb.Counter())

		handler := func() (string, error) {
			return "", ErrBadRequest
		}

		for i := 0; i < 4; i++ {
			_, err := circuit.Query(cb, handler)
			assert.NotNil(err)
			assert.Equal(ErrBadRequest, err)
		}
		_, err := circuit.Query(cb, handler)
		assert.NotNil(err)
		assert.Equal(circuit.ErrCircuitOpen, err)

		assert.Equal(circuit.StateOpen, cb.State())

		// Sleep until timeout ends and state changes from
		// open to half-open.
		sleep := cb.AllowAt().Sub(time.Now())
		time.Sleep(sleep)

		_, err = circuit.Query(cb, handler)
		assert.NotNil(err)

		// Reverts back to open.
		assert.Equal(circuit.StateOpen, cb.State())
	})
}
