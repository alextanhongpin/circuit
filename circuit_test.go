package circuit_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alextanhongpin/circuit"
	"github.com/stretchr/testify/assert"
)

var ErrBadRequest = errors.New("bad request")

func TestState(t *testing.T) {
	assert := assert.New(t)

	assert.Equal("closed", circuit.StateClosed.String())
	assert.Equal("open", circuit.StateOpen.String())
	assert.Equal("half-open", circuit.StateHalfOpen.String())
	assert.Equal("", circuit.State(99).String())
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

	assert.Nil(circuit.Exec(cb, okHandler))

	for i := 0; i < 3; i++ {
		go func() {
			defer errWg.Done()

			assert.Equal(ErrBadRequest, circuit.Exec(cb, errHandler))
			assert.True(cb.State().IsClosed())
		}()
	}
	errWg.Wait()

	assert.Equal(ErrBadRequest, circuit.Exec(cb, errHandler))
	assert.True(cb.State().IsOpen())

	assert.Equal(circuit.ErrUnavailable, circuit.Exec(cb, errHandler))
	assert.True(cb.State().IsOpen())

	time.Sleep(cb.AllowAt().Sub(time.Now()))

	var okWg sync.WaitGroup
	okWg.Add(3)

	for i := 0; i < 3; i++ {
		go func() {
			defer okWg.Done()

			assert.Nil(circuit.Exec(cb, okHandler))
			assert.True(cb.State().IsHalfOpen())
		}()
	}
	okWg.Wait()

	assert.Nil(circuit.Exec(cb, okHandler))
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
		assert.Equal(circuit.ErrUnavailable, err)

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
