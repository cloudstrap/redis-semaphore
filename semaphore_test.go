package rsemaphore

import (
	"testing"

	"time"

	"runtime/debug"

	"fmt"
	"sync"

	"github.com/go-redis/redis"
)

var rc = redis.NewUniversalClient(&redis.UniversalOptions{
	Addrs:       []string{"localhost:6379"},
	PoolTimeout: 2 * time.Minute,
	PoolSize:    250,
})

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func cleanup(s *Semaphore) {
	check(rc.Del(s.key(defaultAvailableKey), s.key(defaultGrabbedKey), s.key(defaultInitializedKey)).Err())
}

func checkLen(expected int64, s *Semaphore, t *testing.T) {
	c, err := rc.LLen(s.key(defaultAvailableKey)).Result()
	check(err)

	if c != expected {
		debug.PrintStack()
		t.Fatal(c, " != ", expected)
	}
}

func TestInit(t *testing.T) {
	s := New(RedisClient(rc), Size(5))
	defer cleanup(s)
	checkLen(5, s, t)
}

func TestInitConcurrency(t *testing.T) {
	s := New(RedisClient(rc), Size(5))
	defer cleanup(s)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			New(RedisClient(rc), Size(5))
		}()
	}

	wg.Wait()
	checkLen(5, s, t)
}

func TestAcquireSimple(t *testing.T) {
	s := New(RedisClient(rc), Size(5))
	defer cleanup(s)
	for i := 0; i < 5; i++ {
		check(s.Acquire(1 * time.Second))
	}
}

func TestTryAcquireSimple(t *testing.T) {
	s := New(RedisClient(rc), Size(5))
	defer cleanup(s)
	for i := 0; i < 5; i++ {
		if b, _ := s.TryAcquire(); !b {
			t.Fatal("tryacquire")
		}
	}
}

func TestAcquireShouldTimeout(t *testing.T) {
	s := New(RedisClient(rc), Size(5))
	defer cleanup(s)
	for i := 0; i < 5; i++ {
		check(s.Acquire(1 * time.Second))
	}

	err := s.Acquire(1 * time.Second)
	if err == nil {
		t.Fatal("acquired")
	} else if err != ErrTimeout {
		t.Log(err)
	}
}

func TestTryAcquireShouldFalse(t *testing.T) {
	s := New(RedisClient(rc), Size(5))
	defer cleanup(s)
	for i := 0; i < 5; i++ {
		check(s.Acquire(1 * time.Second))
	}

	b, err := s.TryAcquire()
	check(err)
	if b {
		t.Fatal("acquired")
	}
}

func TestRelease(t *testing.T) {
	s := New(RedisClient(rc), Size(5))
	defer cleanup(s)
	for i := 0; i < 5; i++ {
		check(s.Acquire(1 * time.Second))
		checkLen(int64(5-i-1), s, t)
	}

	for i := 0; i < 5; i++ {
		check(s.Release())
		checkLen(int64(i+1), s, t)
	}

	checkLen(5, s, t)
}

func TestAcquireConcurrency(t *testing.T) {
	var tests = []struct {
		concurrency int
		size        int64
	}{
		{250, 1},
		{250, 5},
		{250, 10},
		{10, 1},
		{10, 5},
		{10, 10},
		{1, 1},
		{1, 5},
		{1, 10},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestAcquireConcurrency %d - %d", tt.concurrency, tt.size), func(t *testing.T) {
			s := New(RedisClient(rc), Size(tt.size))
			defer cleanup(s)
			var wg sync.WaitGroup
			var ii int

			for i := 0; i < tt.concurrency; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					check(s.Acquire(0))
					ii++
					check(s.Release())
				}()
			}

			wg.Wait()

			r, err := s.opts.rc.HGetAll(s.key(defaultGrabbedKey)).Result()
			check(err)
			if len(r) != 0 {
				t.Fatal("leftover workers")
			}

			checkLen(tt.size, s, t)
		})
	}
}

func TestStaleWorkers(t *testing.T) {
	s := New(RedisClient(rc), Size(5), StaleTimeout(1*time.Second))
	defer cleanup(s)

	go func() {
		check(s.Acquire(0))
	}()

	<-time.After(100 * time.Millisecond)
	r, err := s.opts.rc.HGetAll(s.key(defaultGrabbedKey)).Result()
	check(err)
	if len(r) != 1 {
		t.Fatal("has to be 1 worker")
	}
	check(s.CleanupStaleWorkers())
	r, err = s.opts.rc.HGetAll(s.key(defaultGrabbedKey)).Result()
	check(err)
	if len(r) != 1 {
		t.Fatal("has to be 1 worker")
	}

	<-time.After(1 * time.Second)
	check(s.CleanupStaleWorkers())
	r, err = s.opts.rc.HGetAll(s.key(defaultGrabbedKey)).Result()
	check(err)
	if len(r) != 0 {
		t.Fatal("leftover workers")
	}
}
