// Package ratelimit provides two rate-limiting primitives extracted from treasury-api and
// notifications-api, which had independently built overlapping implementations for two
// genuinely different problems:
//
//   - Limiter: a Redis sliding-window request limiter for IP/tenant abuse throttling
//     (treasury-api's original use case).
//   - Quota: a Redis daily usage-quota counter, gated on a JWT-claim-sourced limit, for
//     plan/feature metering (notifications-api's original use case).
//
// Both preserve their source service's exact wire contract (headers, JSON error body,
// fail-open-on-Redis-error posture) — this package is a swap, not a redesign, so migrating
// either service is a like-for-like replacement.
package ratelimit

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Limiter is a Redis sliding-window request limiter (sorted-set log), for abuse throttling by
// IP or tenant. Fails open on any Redis error — the limiter itself must never cause an outage.
type Limiter struct {
	redis  *redis.Client
	logger *zap.Logger
	prefix string
}

// NewLimiter creates a sliding-window Limiter. servicePrefix namespaces its Redis keys
// (e.g. "treasury" -> "rl:treasury:...") so multiple services can share one Redis instance.
func NewLimiter(rdb *redis.Client, logger *zap.Logger, servicePrefix string) *Limiter {
	return &Limiter{
		redis:  rdb,
		logger: logger.Named("ratelimit"),
		prefix: "rl:" + servicePrefix + ":",
	}
}

// Middleware returns an HTTP middleware enforcing `limit` requests per `window` for the key
// keyFunc derives from the request. Sets X-RateLimit-Limit/Remaining/Reset on every response;
// on rejection, a 429 with a JSON `{"error":"rate limit exceeded","message":"..."}` body.
func (l *Limiter) Middleware(keyFunc func(r *http.Request) string, limit int, window time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if l.redis == nil {
				next.ServeHTTP(w, r)
				return
			}

			key := l.prefix + keyFunc(r)
			redisCtx, cancel := context.WithTimeout(r.Context(), 200*time.Millisecond)
			defer cancel()

			now := time.Now()
			windowStart := now.Add(-window)

			pipe := l.redis.Pipeline()
			pipe.ZRemRangeByScore(redisCtx, key, "0", strconv.FormatInt(windowStart.UnixMilli(), 10))
			countCmd := pipe.ZCard(redisCtx, key)
			_, err := pipe.Exec(redisCtx)
			if err != nil && err != redis.Nil {
				// Fail open on Redis error
				next.ServeHTTP(w, r)
				return
			}

			count := countCmd.Val()
			remaining := limit - int(count)

			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(limit))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(max(remaining, 0)))
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(now.Add(window).Unix(), 10))

			if remaining <= 0 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"rate limit exceeded","message":"too many requests, please try again later"}`))
				return
			}

			// Record this request
			_ = l.redis.ZAdd(redisCtx, key, redis.Z{
				Score:  float64(now.UnixMilli()),
				Member: fmt.Sprintf("%d", now.UnixNano()),
			}).Err()
			_ = l.redis.Expire(redisCtx, key, window+time.Minute)

			next.ServeHTTP(w, r)
		})
	}
}

// IPKey is a keyFunc that rate-limits by client IP address.
func IPKey(r *http.Request) string {
	return "ip:" + getRealIP(r)
}

// TenantKey returns a keyFunc that rate-limits by the given tenant-ID header, falling back to
// IP when the header is absent.
func TenantKey(headerName string) func(*http.Request) string {
	return func(r *http.Request) string {
		tenant := r.Header.Get(headerName)
		if tenant == "" {
			return "ip:" + getRealIP(r)
		}
		return "tenant:" + tenant
	}
}

func getRealIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		for i := 0; i < len(xff); i++ {
			if xff[i] == ',' {
				return xff[:i]
			}
		}
		return xff
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	ip := r.RemoteAddr
	for i := len(ip) - 1; i >= 0; i-- {
		if ip[i] == ':' {
			return ip[:i]
		}
	}
	return ip
}
