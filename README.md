# shared-ratelimit

Rate limiting for every Go service, safe at any replica count.

- **`Limiter`**: request throttling by IP, user or tenant. GCRA (the token bucket stored as one
  timestamp per key) via `github.com/go-redis/redis_rate/v10`: one atomic Lua call per request
  using the Redis server clock, so all pods sharing one Redis enforce one exact limit.
- **`Quota`**: daily plan/feature metering (limit from JWT claims), one atomic Lua call that
  increments, keeps the TTL and rolls back on rejection. `CheckN` is all-or-nothing for batches.

## Usage

```go
limiter := ratelimit.NewLimiter(redisClient, logger, "myservice") // any redis.UniversalClient

// Global per-IP limit (WebSocket, SSE and health probes are exempt automatically).
router.Use(limiter.Middleware(ratelimit.IPKey, 120, time.Minute))

// Stricter limit for PIN/password/OTP routes: per IP AND per target identifier.
pinKey := ratelimit.CompositeKey(ratelimit.IPKey, func(r *http.Request) string { return "user:" + loginIdentifier(r) })
r.With(limiter.MiddlewareWith(pinKey, ratelimit.Options{Name: "pin", Limit: 8, Window: time.Minute})).Post("/auth/pin", h)

// Tenant/user keys only from verified claims, never raw headers.
byTenant := ratelimit.ValueKey("tenant", func(r *http.Request) string { return claimsTenantID(r.Context()) })

// Outside HTTP (provider send limits, workers):
ok, retryAfter := limiter.Allow(ctx, "smtp:primary", ratelimit.Options{Limit: 200, Window: time.Hour, Name: "provider"}, 1)
```

```go
quota := ratelimit.NewQuota(redisClient)
res, _ := quota.CheckN(ctx, tenantID, "email_notifications_per_day", limit, len(recipients))
router.Use(ratelimit.RequireQuota(quota, "sms_sends", claimsFn))
```

## Behavior

| Topic | Behavior |
|---|---|
| Client IP | `ClientIP` uses `X-Real-IP` (set by ingress-nginx from `CF-Connecting-IP`), then `CF-Connecting-IP`, then the peer address. `X-Forwarded-For` is never trusted (its first entry is client-controlled). Remove chi `middleware.RealIP`, which trusts client headers. |
| Burst | `Options.Burst` (default = Limit). After idling, a client may send Burst requests back to back, then Limit per Window. |
| Headers | `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`, and `Retry-After` on 429 with the JSON body `{"error":"rate limit exceeded",...}`. |
| Redis down | Each pod falls back to an in-process bucket allowing `Limit / ExpectedReplicas` (default 2), bounded to 10,000 keys with LRU eviction, so throttling continues without blocking real users. A warning is logged at most once a minute. |
| Exempt | `SkipStreaming`: WebSocket upgrades, `Accept: text/event-stream`, `/healthz`, `/readyz`, `/livez`, `/health`, `/ready`, `/live`, `/metrics`. Add more with `Options.Skip`. |
| Memory | GCRA keys (`rate:rl:<svc>:<name>:<subject>`) expire when the bucket refills; quota keys after 25h. Redis runs `allkeys-lru`. |
| Mount order | Mount after CORS so 429 responses still carry CORS headers. |

## Install

```bash
go get github.com/Bengo-Hub/shared-ratelimit@v0.2.0
```

## Versions

- v0.2.0: GCRA via redis_rate (atomic across pods), `MiddlewareWith`/`Options`, `Allow`,
  `ClientIP` (no XFF trust), `SkipStreaming`, `ValueKey`, `CompositeKey`, per-pod fallback,
  `UniversalClient`, atomic `Quota` Lua with `CheckN`, configurable `UpgradeURL`.
- v0.1.0: sliding-window log (non-atomic check and record; do not use).
