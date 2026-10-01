// Package ratelimit provides the fleet's two rate-limiting primitives:
//
//   - Limiter: request throttling by IP, user or tenant, shared by every replica through Redis.
//   - Quota: a daily usage counter for plan/feature metering, with the limit taken from the
//     caller (usually JWT claims).
//
// Limiter uses GCRA (generic cell rate algorithm, the token bucket expressed as one timestamp
// per key) via github.com/go-redis/redis_rate. The whole check-and-consume step is one Lua
// script run atomically by Redis using the Redis server clock, so N pods sharing one Redis
// enforce one limit exactly, with one round trip and one small key per subject. The previous
// sorted-set log counted and recorded in separate calls, so parallel requests across pods
// could all pass.
package ratelimit

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// KeyFunc derives the rate-limit subject from a request. Return "" to skip limiting it.
type KeyFunc func(r *http.Request) string

// Options configures one limiter middleware.
type Options struct {
	// Limit requests are allowed per Window on average.
	Limit  int
	Window time.Duration
	// Burst is how many requests may arrive back to back after an idle period. Defaults to
	// Limit, which behaves like "Limit per Window" for a client starting cold.
	Burst int
	// Name separates two limiters that share a key function (for example a global per-IP
	// limit and a stricter per-IP limit on PIN login). Defaults to "default".
	Name string
	// Cost returns how many units a request consumes. Defaults to 1.
	Cost func(r *http.Request) int
	// Skip exempts requests in addition to the built-in SkipStreaming.
	Skip func(r *http.Request) bool
	// ExpectedReplicas sizes the in-process fallback used while Redis is unreachable: each
	// pod then allows Limit/ExpectedReplicas, so the fleet stays near the real limit.
	// Defaults to 2 (the platform minimum).
	ExpectedReplicas int
}

// Limiter enforces request limits shared across every replica of a service.
type Limiter struct {
	rl     *redis_rate.Limiter
	logger *zap.Logger
	prefix string

	fallback     *expirable.LRU[string, *rate.Limiter]
	fallbackMu   sync.Mutex
	lastWarnUnix int64
}

// NewLimiter creates a Limiter. servicePrefix namespaces its keys (e.g. "treasury") so services
// can share one Redis. rdb may be nil, in which case only the in-process fallback runs.
func NewLimiter(rdb redis.UniversalClient, logger *zap.Logger, servicePrefix string) *Limiter {
	if logger == nil {
		logger = zap.NewNop()
	}
	l := &Limiter{
		logger:   logger.Named("ratelimit"),
		prefix:   "rl:" + servicePrefix + ":",
		fallback: expirable.NewLRU[string, *rate.Limiter](10000, nil, 10*time.Minute),
	}
	if !isNil(rdb) {
		l.rl = redis_rate.NewLimiter(rdb)
	}
	return l
}

// Middleware enforces `limit` requests per `window` per key. Kept for v0.1 callers; new code
// can use MiddlewareWith for burst, cost, named limiters and extra exemptions.
func (l *Limiter) Middleware(keyFunc func(r *http.Request) string, limit int, window time.Duration) func(http.Handler) http.Handler {
	return l.MiddlewareWith(keyFunc, Options{Limit: limit, Window: window})
}

// MiddlewareWith returns the limiter middleware for opts.
//
// Every response carries X-RateLimit-Limit, X-RateLimit-Remaining and X-RateLimit-Reset
// (unix seconds when the bucket is full again). A rejection is a 429 with Retry-After and the
// JSON body {"error":"rate limit exceeded","message":"..."}.
func (l *Limiter) MiddlewareWith(keyFunc func(r *http.Request) string, opts Options) func(http.Handler) http.Handler {
	opts = normalize(opts)
	limit := redis_rate.Limit{Rate: opts.Limit, Burst: opts.Burst, Period: opts.Window}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if SkipStreaming(r) || (opts.Skip != nil && opts.Skip(r)) {
				next.ServeHTTP(w, r)
				return
			}
			subject := keyFunc(r)
			if subject == "" {
				next.ServeHTTP(w, r)
				return
			}
			key := l.prefix + opts.Name + ":" + subject
			cost := 1
			if opts.Cost != nil {
				if c := opts.Cost(r); c > 0 {
					cost = c
				}
			}

			allowed, remaining, retryAfter, resetAfter := l.allow(r.Context(), key, limit, cost, opts)

			h := w.Header()
			h.Set("X-RateLimit-Limit", strconv.Itoa(opts.Limit))
			h.Set("X-RateLimit-Remaining", strconv.Itoa(max(remaining, 0)))
			h.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(resetAfter).Unix(), 10))
			if !allowed {
				secs := int(retryAfter.Round(time.Second) / time.Second)
				h.Set("Retry-After", strconv.Itoa(max(secs, 1)))
				h.Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"rate limit exceeded","message":"too many requests, please try again later"}`))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// Allow checks and consumes cost units for key outside HTTP (workers, provider send limits).
func (l *Limiter) Allow(ctx context.Context, key string, opts Options, cost int) (allowed bool, retryAfter time.Duration) {
	opts = normalize(opts)
	limit := redis_rate.Limit{Rate: opts.Limit, Burst: opts.Burst, Period: opts.Window}
	if cost < 1 {
		cost = 1
	}
	ok, _, ra, _ := l.allow(ctx, l.prefix+opts.Name+":"+key, limit, cost, opts)
	return ok, ra
}

func (l *Limiter) allow(ctx context.Context, key string, limit redis_rate.Limit, cost int, opts Options) (bool, int, time.Duration, time.Duration) {
	if l.rl != nil {
		rctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		res, err := l.rl.AllowN(rctx, key, limit, cost)
		cancel()
		if err == nil {
			return res.Allowed > 0, res.Remaining, res.RetryAfter, res.ResetAfter
		}
		l.warnFallback(err)
	}
	return l.allowLocal(key, opts, cost)
}

// allowLocal is the per-pod token bucket used while Redis is unreachable.
func (l *Limiter) allowLocal(key string, opts Options, cost int) (bool, int, time.Duration, time.Duration) {
	share := float64(opts.Limit) / float64(opts.ExpectedReplicas)
	burst := max(opts.Burst/opts.ExpectedReplicas, 1)
	every := rate.Limit(share / opts.Window.Seconds())

	l.fallbackMu.Lock()
	b, ok := l.fallback.Get(key)
	if !ok {
		b = rate.NewLimiter(every, burst)
		l.fallback.Add(key, b)
	}
	l.fallbackMu.Unlock()

	now := time.Now()
	res := b.ReserveN(now, cost)
	if !res.OK() {
		return false, 0, opts.Window, opts.Window
	}
	if d := res.DelayFrom(now); d > 0 {
		res.CancelAt(now)
		return false, 0, d, d
	}
	return true, int(b.TokensAt(now)), 0, opts.Window
}

func (l *Limiter) warnFallback(err error) {
	now := time.Now().Unix()
	l.fallbackMu.Lock()
	defer l.fallbackMu.Unlock()
	if now-l.lastWarnUnix < 60 {
		return
	}
	l.lastWarnUnix = now
	l.logger.Warn("rate limit redis unavailable, using per-pod fallback", zap.Error(err))
}

func normalize(o Options) Options {
	if o.Limit < 1 {
		o.Limit = 1
	}
	if o.Window <= 0 {
		o.Window = time.Minute
	}
	if o.Burst < 1 {
		o.Burst = o.Limit
	}
	if o.Name == "" {
		o.Name = "default"
	}
	if o.ExpectedReplicas < 1 {
		o.ExpectedReplicas = 2
	}
	return o
}

// SkipStreaming exempts long-lived and infrastructure requests: WebSocket upgrades, SSE
// streams (EventSource reconnects must never be throttled into a reconnect storm) and health
// or metrics probes (kubelet probes share a node IP and must never see a 429).
func SkipStreaming(r *http.Request) bool {
	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return true
	}
	if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		return true
	}
	switch r.URL.Path {
	case "/healthz", "/health", "/readyz", "/livez", "/metrics", "/ready", "/live":
		return true
	}
	return false
}

// ClientIP returns the caller's IP as established by the ingress, never by the caller.
//
// ingress-nginx runs with forwarded-for-header CF-Connecting-IP and proxy-real-ip-cidr set to
// Cloudflare's ranges, so it sets X-Real-IP to the real client. X-Forwarded-For is NOT used:
// Cloudflare appends to whatever the client sent, so its first entry is attacker-controlled.
// In-cluster calls (no ingress) fall back to the peer address.
func ClientIP(r *http.Request) string {
	if ip := cleanIP(r.Header.Get("X-Real-IP")); ip != "" {
		return ip
	}
	if ip := cleanIP(r.Header.Get("CF-Connecting-IP")); ip != "" {
		return ip
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func cleanIP(v string) string {
	v = strings.TrimSpace(v)
	if v == "" || net.ParseIP(v) == nil {
		return ""
	}
	return v
}

// IPKey rate-limits by client IP.
func IPKey(r *http.Request) string {
	return "ip:" + ClientIP(r)
}

// TenantKey rate-limits by a tenant-ID header, falling back to IP.
//
// Deprecated: a raw header is caller-controlled, so anyone can spend another tenant's
// allowance. Use ValueKey with the tenant taken from verified claims or the resolved route.
func TenantKey(headerName string) func(*http.Request) string {
	return func(r *http.Request) string {
		tenant := r.Header.Get(headerName)
		if tenant == "" {
			return IPKey(r)
		}
		return "tenant:" + tenant
	}
}

// ValueKey builds a key from a trusted value (verified tenant or user ID from request
// context), falling back to the client IP when the value is empty.
func ValueKey(kind string, value func(r *http.Request) string) func(*http.Request) string {
	return func(r *http.Request) string {
		if v := value(r); v != "" {
			return kind + ":" + v
		}
		return IPKey(r)
	}
}

// CompositeKey joins several keys, e.g. IP plus the submitted login identifier, so a brute
// force is limited per target account from each source. Any empty part skips limiting.
func CompositeKey(parts ...func(*http.Request) string) func(*http.Request) string {
	return func(r *http.Request) string {
		vals := make([]string, 0, len(parts))
		for _, p := range parts {
			v := p(r)
			if v == "" {
				return ""
			}
			vals = append(vals, v)
		}
		return strings.Join(vals, "|")
	}
}

func isNil(rdb redis.UniversalClient) bool {
	if rdb == nil {
		return true
	}
	switch c := rdb.(type) {
	case *redis.Client:
		return c == nil
	case *redis.ClusterClient:
		return c == nil
	case *redis.Ring:
		return c == nil
	}
	return false
}
