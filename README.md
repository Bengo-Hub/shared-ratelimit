# shared-ratelimit

Two rate-limiting primitives, extracted from treasury-api and notifications-api, which had
independently built overlapping implementations for two genuinely different problems:

- **`Limiter`** — a Redis sliding-window request limiter, for IP/tenant abuse throttling.
- **`Quota`** — a Redis daily usage-quota counter, for plan/feature metering sourced from a
  per-request limit (typically a JWT claim).

Both preserve their source service's exact wire contract (headers, JSON error body, fail-open
posture) — adopting this package is a swap, not a redesign.

## Usage

### Abuse throttling (IP or tenant)

```go
limiter := ratelimit.NewLimiter(redisClient, logger, "myservice")
router.Use(limiter.Middleware(ratelimit.IPKey, 120, time.Minute))
// or: limiter.Middleware(ratelimit.TenantKey("X-Tenant-ID"), 120, time.Minute)
```

### Daily plan/feature quota

```go
quota := ratelimit.NewQuota(redisClient)
router.Use(ratelimit.RequireQuota(quota, "sms_sends", func(ctx context.Context) (string, int, bool) {
    claims, ok := authclient.ClaimsFromContext(ctx)
    if !ok {
        return "", 0, false
    }
    return claims.TenantID, claims.GetLimit("sms_sends"), true
}))
```

## Install

```bash
go get github.com/Bengo-Hub/shared-ratelimit@v0.1.0
```
