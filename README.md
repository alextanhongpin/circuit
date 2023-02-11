# circuit

Concurrent-safe circuit breaker written in Golang.


## Usage

```go
package main

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/alextanhongpin/circuit"
)

func main() {
	opt := &circuit.Option{
		Success: 3,
		Failure: 3,
		Now:     time.Now,
		Timeout: 1 * time.Second,
	}
	cb := circuit.New(opt)

	// Use Query if your handler returns both response and error.
	res, err := circuit.Query(cb, queryHandler)
	if err != nil {
		panic(err)
	}

	log.Println(res)
	log.Println(cb.State().String()) // closed

	// Simulate circuit breaker transition from closed to open.
	var wg sync.WaitGroup
	wg.Add(4)

	for i := 0; i < 4; i++ {
		go func() {
			defer wg.Done()

			// Use Exec if your handler only returns error.
			log.Println(cb.Exec(execHandler))
		}()
	}
	wg.Wait()

	log.Println(cb.State().String()) // open

	// Wait for circuit breaker to recover.
	sleep := cb.AllowAt().Sub(time.Now())
	time.Sleep(sleep)

	// Simulate successful handler after the circuit breaker timeouts.
	log.Println(cb.Exec(noopHandler))
	log.Println(cb.State().String()) // half-open.

	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()

			log.Println(cb.Exec(noopHandler))
		}()
	}
	wg.Wait()

	log.Println(cb.State().String()) // open.
}

func queryHandler() (string, error) {
	return "hello world", nil
}

func execHandler() error {
	return errors.New("bad request")
}

func noopHandler() error {
	return nil
}
```

Output:
```
2023/02/11 09:44:34 hello world
2023/02/11 09:44:34 closed
2023/02/11 09:44:34 bad request
2023/02/11 09:44:34 bad request
2023/02/11 09:44:34 bad request
2023/02/11 09:44:34 bad request
2023/02/11 09:44:34 open
2023/02/11 09:44:35 <nil>
2023/02/11 09:44:35 half-open
2023/02/11 09:44:35 <nil>
2023/02/11 09:44:35 <nil>
2023/02/11 09:44:35 <nil>
2023/02/11 09:44:35 closed
```
