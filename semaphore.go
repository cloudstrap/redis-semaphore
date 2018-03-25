package rsemaphore

import (
	"strings"
	"sync"
	"time"

	"strconv"

	"fmt"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
	"github.com/rs/xid"
)

const (
	DefaultPrefix         = "redis_semaphore"
	defaultAvailableKey   = "available"
	defaultGrabbedKey     = "grabbed"
	defaultInitializedKey = "initialized"
)

var (
	ErrTimeout = errors.New("semaphore: timeout")
)

type Semaphore struct {
	opts *SemaphoreOptions

	keyAvailable   string
	keyGrabbed     string
	keyInitialized string

	tokens []string
	m      sync.Mutex
}

func (s *Semaphore) init() (error error) {
	defer func() {
		if error != nil {
			//something failed, return the state of the initialized key
			s.opts.rc.GetSet(s.keyInitialized, 0).Int64()
		} else {
			//try and cleanup stale workers in any case
			if err := s.CleanupStaleWorkers(); err != nil {
				error = err
				return
			}
		}
	}()

	//check if the initialized key is set, if it is, that means someone is already initializing the semaphore
	set, err := s.opts.rc.SetNX(s.keyInitialized, 1, s.opts.initKeyExpiration).Result()
	if err != nil {
		error = errors.Wrap(err, "set initialized key")
		return
	}

	if !set {
		return
	}

	//if the semaphore does not appear to be initialized (never initialized or key is expired)
	//check if the workers queue is available.
	//It's guaranteed to be available when the semaphore is actually initialized since it's executed in a transaction
	if exists, err := s.opts.rc.Exists(s.keyAvailable).Result(); err != nil {
		error = errors.Wrap(err, "keyAvailable exists")
		return
	} else if exists == 1 {
		return
	}

	//push the workers in a transaction
	pushPipe := s.opts.rc.Pipeline()
	var i int64
	for i = 0; i < s.opts.size; i++ {
		pushPipe.LPush(s.keyAvailable, s.generateToken())
	}
	pushPipe.Expire(s.keyAvailable, s.opts.keysExpiration)
	pushPipe.LTrim(s.keyAvailable, 0, s.opts.size-1)

	if _, err := pushPipe.Exec(); err != nil {
		error = errors.Wrap(err, "pushing worker tokens")
		return
	}

	return
}

func (s *Semaphore) generateToken() string {
	return xid.New().String()
}

func (s *Semaphore) acquireToken(token string) error {
	pipe := s.opts.rc.Pipeline()
	pipe.HSet(s.keyGrabbed, token, time.Now().UnixNano())
	pipe.Expire(s.keyGrabbed, s.opts.keysExpiration)

	if _, err := pipe.Exec(); err != nil {
		return errors.Wrap(err, "set get grabbed")
	}

	s.m.Lock()
	s.tokens = append(s.tokens, token)
	s.m.Unlock()

	return nil
}

func (s *Semaphore) key(key string) string {
	return strings.Join([]string{s.opts.prefix, key, fmt.Sprintf("%d", s.opts.size)}, "_")
}

func (s *Semaphore) Acquire(timeout time.Duration) error {
	res, err := s.opts.rc.BLPop(timeout, s.keyAvailable).Result()
	if err != nil {
		if err == redis.Nil {
			return ErrTimeout
		}

		return err
	}
	token := res[1]
	return s.acquireToken(token)
}

func (s *Semaphore) Release() error {
	s.m.Lock()
	if len(s.tokens) == 0 {
		panic("semaphore: negative release")
	}

	token := s.tokens[0]
	s.tokens = s.tokens[1:]
	s.m.Unlock()

	pipe := s.opts.rc.Pipeline()
	pipe.RPush(s.keyAvailable, token)
	pipe.HDel(s.keyGrabbed, token)

	if _, err := pipe.Exec(); err != nil {
		return err
	}

	return nil
}

func (s *Semaphore) TryAcquire() (bool, error) {
	res, err := s.opts.rc.LPop(s.keyAvailable).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}

		return false, err
	} else if res == "" {
		return false, nil
	}
	if err := s.acquireToken(res); err != nil {
		return false, err
	}

	return true, nil
}

func (s *Semaphore) Available() (int64, error) {
	return s.opts.rc.LLen(s.keyAvailable).Result()
}

func (s *Semaphore) CleanupStaleWorkers() error {
	//cleanup stale keys, if any
	grabbed, err := s.opts.rc.HGetAll(s.keyGrabbed).Result()
	if err != nil {
		return errors.Wrap(err, "get grabbed keys")
	}

	for k, v := range grabbed {
		unix, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return errors.Wrap(err, "parsing grabbed key timestamp")
		}

		t := time.Unix(0, unix)
		if t.Add(s.opts.staleTimeout).Before(time.Now()) {
			if err := s.opts.rc.HDel(s.keyGrabbed, k).Err(); err != nil {
				return errors.Wrap(err, "deleting stale key")
			}

			if err := s.opts.rc.LPush(s.keyAvailable, k).Err(); err != nil {
				return errors.Wrap(err, "pushing back stale key to queue")
			}
		}
	}

	return nil
}

func New(opts ...SemaphoreOption) *Semaphore {
	s := &Semaphore{
		opts: &SemaphoreOptions{},
	}

	for _, o := range opts {
		o(s.opts)
	}

	if s.opts.size == 0 {
		s.opts.size = 5
	}

	if s.opts.prefix == "" {
		s.opts.prefix = DefaultPrefix
	}

	if s.opts.staleTimeout == 0 {
		s.opts.staleTimeout = 10 * time.Minute
	}

	if s.opts.keysExpiration == 0 {
		s.opts.keysExpiration = 24 * time.Hour
	}

	if s.opts.initKeyExpiration == 0 {
		s.opts.initKeyExpiration = 30 * time.Second
	}

	if s.opts.initRetry == 0 {
		s.opts.initRetry = 5
	}

	if s.opts.rc == nil {
		if s.opts.ro == nil {
			panic("semaphore: supply redis opts")
		}

		s.opts.rc = redis.NewUniversalClient(s.opts.ro)
	}

	s.tokens = make([]string, 0, s.opts.size)
	s.keyAvailable = s.key(defaultAvailableKey)
	s.keyGrabbed = s.key(defaultGrabbedKey)
	s.keyInitialized = s.key(defaultInitializedKey)

	var initErr error
	for i := 0; i < s.opts.initRetry; i++ {
		if err := s.init(); err != nil {
			initErr = err
		} else {
			initErr = nil
			break
		}
	}

	if initErr != nil {
		panic(errors.Wrap(initErr, "semaphore: init"))
	}

	return s
}
