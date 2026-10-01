package ratelimit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func newRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
}

// Four limiter instances sharing one Redis stand in for four pods behind a load balancer.
// 200 parallel requests at limit 50 must admit exactly 50 in total.
func TestLimiterExactAcrossPods(t *testing.T) {
	_, rdb := newRedis(t)
	var pods []http.Handler
	for i := 0; i < 4; i++ {
		l := NewLimiter(rdb, zap.NewNop(), "svc")
		pods = append(pods, l.Middleware(IPKey, 50, time.Hour)(okHandler()))
	}
	var allowed, rejected int32
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/api/x", nil)
			req.Header.Set("X-Real-IP", "203.0.113.7")
			rec := httptest.NewRecorder()
			pods[i%4].ServeHTTP(rec, req)
			if rec.Code == http.StatusOK {
				atomic.AddInt32(&allowed, 1)
			} else if rec.Code == http.StatusTooManyRequests {
				atomic.AddInt32(&rejected, 1)
				if rec.Header().Get("Retry-After") == "" {
					t.Error("429 without Retry-After")
				}
			}
		}(i)
	}
	wg.Wait()
	if allowed != 50 || rejected != 150 {
		t.Fatalf("allowed=%d rejected=%d, want 50/150", allowed, rejected)
	}
}

func TestClientIPIgnoresSpoofedForwardedFor(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.5:4444"
	req.Header.Set("X-Forwarded-For", "1.1.1.1, 198.51.100.9")
	req.Header.Set("X-Real-IP", "198.51.100.9")
	if got := ClientIP(req); got != "198.51.100.9" {
		t.Fatalf("got %s, want ingress X-Real-IP", got)
	}

	req.Header.Del("X-Real-IP")
	if got := ClientIP(req); got != "10.0.0.5" {
		t.Fatalf("without ingress headers must use peer address, got %s", got)
	}

	req.Header.Set("X-Real-IP", "not-an-ip")
	if got := ClientIP(req); got != "10.0.0.5" {
		t.Fatalf("garbage X-Real-IP must be ignored, got %s", got)
	}
}

func TestSkipStreamingAndHealth(t *testing.T) {
	_, rdb := newRedis(t)
	h := NewLimiter(rdb, nil, "svc").Middleware(IPKey, 1, time.Hour)(okHandler())
	for _, mk := range []func() *http.Request{
		func() *http.Request { return httptest.NewRequest(http.MethodGet, "/healthz", nil) },
		func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/stream", nil)
			r.Header.Set("Accept", "text/event-stream")
			return r
		},
		func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/ws", nil)
			r.Header.Set("Upgrade", "websocket")
			return r
		},
	} {
		for i := 0; i < 3; i++ {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, mk())
			if rec.Code != http.StatusOK {
				t.Fatalf("exempt request got %d", rec.Code)
			}
		}
	}
}

func TestFallbackWhenRedisDown(t *testing.T) {
	mr, rdb := newRedis(t)
	mr.Close()
	l := NewLimiter(rdb, zap.NewNop(), "svc")
	h := l.MiddlewareWith(IPKey, Options{Limit: 10, Window: time.Hour, ExpectedReplicas: 2})(okHandler())
	ok := 0
	for i := 0; i < 20; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api", nil)
		req.Header.Set("X-Real-IP", "203.0.113.8")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code == http.StatusOK {
			ok++
		}
	}
	if ok != 5 {
		t.Fatalf("fallback should allow limit/replicas = 5, allowed %d", ok)
	}
}

func TestCompositeKeyEmptyPartSkips(t *testing.T) {
	k := CompositeKey(IPKey, func(*http.Request) string { return "" })
	if k(httptest.NewRequest(http.MethodGet, "/", nil)) != "" {
		t.Fatal("empty part should skip limiting")
	}
}

func TestQuotaCheckNAllOrNothing(t *testing.T) {
	mr, rdb := newRedis(t)
	q := NewQuota(rdb)
	ctx := context.Background()

	r, _ := q.CheckN(ctx, "t1", "email", 10, 8)
	if !r.Allowed || r.Used != 8 {
		t.Fatalf("first batch: %+v", r)
	}
	r, _ = q.CheckN(ctx, "t1", "email", 10, 5)
	if r.Allowed || r.Used != 8 {
		t.Fatalf("over-limit batch must consume nothing: %+v", r)
	}
	r, _ = q.Check(ctx, "t1", "email", 10)
	if !r.Allowed || r.Used != 9 || r.Remaining != 1 {
		t.Fatalf("single check: %+v", r)
	}
	for _, k := range mr.Keys() {
		if mr.TTL(k) <= 0 {
			t.Fatalf("quota key %s has no TTL", k)
		}
	}
}

func TestTrustedRealIP(t *testing.T) {
	var got string
	h := TrustedRealIP(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = r.RemoteAddr }))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.9:1234"
	req.Header.Set("True-Client-IP", "6.6.6.6")
	req.Header.Set("X-Forwarded-For", "6.6.6.6")
	req.Header.Set("X-Real-IP", "198.51.100.4")
	h.ServeHTTP(httptest.NewRecorder(), req)
	if got != "198.51.100.4:0" {
		t.Fatalf("got %s", got)
	}
}
