package rsemaphore

import (
	"time"

	"github.com/go-redis/redis"
)

type SemaphoreOptions struct {
	ro                *redis.UniversalOptions
	rc                redis.UniversalClient
	prefix            string
	staleTimeout      time.Duration
	keysExpiration    time.Duration
	initKeyExpiration time.Duration
	initRetry         int

	size int64
}

type SemaphoreOption func(*SemaphoreOptions)

var (
	RedisOptions = func(ro *redis.UniversalOptions) SemaphoreOption {
		return func(o *SemaphoreOptions) {
			o.ro = ro
		}
	}

	RedisClient = func(rc redis.UniversalClient) SemaphoreOption {
		return func(o *SemaphoreOptions) {
			o.rc = rc
		}
	}

	KeyPrefix = func(prefix string) SemaphoreOption {
		return func(o *SemaphoreOptions) {
			o.prefix = prefix
		}
	}

	StaleTimeout = func(st time.Duration) SemaphoreOption {
		return func(o *SemaphoreOptions) {
			o.staleTimeout = st
		}
	}

	KeysExpiration = func(st time.Duration) SemaphoreOption {
		return func(o *SemaphoreOptions) {
			o.keysExpiration = st
		}
	}

	InitRetry = func(r int) SemaphoreOption {
		return func(o *SemaphoreOptions) {
			o.initRetry = r
		}
	}

	Size = func(size int64) SemaphoreOption {
		return func(o *SemaphoreOptions) {
			o.size = size
		}
	}

	InitKeyExpiration = func(t time.Duration) SemaphoreOption {
		return func(o *SemaphoreOptions) {
			o.initKeyExpiration = t
		}
	}
)
