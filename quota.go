package ratelimit

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

// Quota is a Redis daily usage-quota counter, for plan/feature metering sourced from a caller-
// supplied per-request limit (typically read from JWT claims) rather than a fixed config value.
// Keys are bucketed by calendar day (UTC) and expire after ~25h; every known caller is daily.
type Quota struct {
	redis redis.UniversalClient
	// UpgradeURL is returned in the 429 body of RequireQuota.
	UpgradeURL string
}

// DefaultUpgradeURL is where RequireQuota points users who hit their plan limit.
const DefaultUpgradeURL = "https://pricingapi.codevertexafrica.com/upgrade"

// NewQuota creates a new Redis-backed daily quota counter.
func NewQuota(rdb redis.UniversalClient) *Quota {
	if isNil(rdb) {
		rdb = nil
	}
	return &Quota{redis: rdb, UpgradeURL: DefaultUpgradeURL}
}

// QuotaResult is the result of a Check call.
type QuotaResult struct {
	Allowed   bool   `json:"allowed"`
	Feature   string `json:"feature"`
	Limit     int    `json:"limit"`
	Used      int    `json:"used"`
	Remaining int    `json:"remaining"`
}

// quotaScript adds ARGV[1] units and keeps the 25h TTL in one atomic step. If the total would
// pass the limit (ARGV[2]) the units are taken back and {0, used} is returned, else {1, used}.
// The TTL is (re)applied whenever the key has none, which also heals keys written by the old
// INCR-then-EXPIRE code if that crashed between the two calls.
var quotaScript = redis.NewScript(`
local n = redis.call("INCRBY", KEYS[1], ARGV[1])
if redis.call("TTL", KEYS[1]) < 0 then
  redis.call("EXPIRE", KEYS[1], ARGV[3])
end
if n > tonumber(ARGV[2]) then
  n = redis.call("DECRBY", KEYS[1], ARGV[1])
  return {0, n}
end
return {1, n}`)

const quotaTTLSeconds = 25 * 60 * 60

// Check checks whether scopeID (typically a tenant ID) is within `limit` uses of `feature`
// today and consumes one use. A limit of -1 means unlimited; 0 means not configured (allow).
func (q *Quota) Check(ctx context.Context, scopeID, feature string, limit int) (*QuotaResult, error) {
	return q.CheckN(ctx, scopeID, feature, limit, 1)
}

// CheckN consumes n uses at once, all or nothing: a batch send to 20 recipients either fits
// in today's quota entirely or consumes nothing. Fails open (allowed) when Redis errors.
func (q *Quota) CheckN(ctx context.Context, scopeID, feature string, limit, n int) (*QuotaResult, error) {
	if limit < 0 {
		return &QuotaResult{Allowed: true, Feature: feature, Limit: -1, Used: 0, Remaining: -1}, nil
	}
	if limit == 0 {
		return &QuotaResult{Allowed: true, Feature: feature, Limit: 0, Used: 0, Remaining: 0}, nil
	}
	if n < 1 {
		n = 1
	}
	if q.redis == nil {
		return &QuotaResult{Allowed: true, Feature: feature, Limit: limit, Remaining: limit}, nil
	}

	key := fmt.Sprintf("ratelimit:%s:%s:%s", scopeID, feature, time.Now().UTC().Format("2006-01-02"))
	vals, err := quotaScript.Run(ctx, q.redis, []string{key}, n, limit, quotaTTLSeconds).Int64Slice()
	if err != nil || len(vals) != 2 {
		return &QuotaResult{Allowed: true, Feature: feature, Limit: limit, Used: 0, Remaining: limit}, nil
	}
	used := int(vals[1])
	return &QuotaResult{
		Allowed:   vals[0] == 1,
		Feature:   feature,
		Limit:     limit,
		Used:      used,
		Remaining: max(limit-used, 0),
	}, nil
}

// ClaimsFunc resolves the scope ID (tenant ID) and configured limit for featureKey from the
// request context (typically JWT claims). ok=false skips quota enforcement entirely (e.g. no
// claims in context, such as an S2S request) — the same "don't block" posture the original
// per-service implementations used.
type ClaimsFunc func(ctx context.Context) (scopeID string, limit int, ok bool)

// RequireQuota returns middleware enforcing a daily quota for featureKey, resolved per-request
// via claimsFn — kept as a callback (rather than importing shared-auth-client directly) so this
// package has no dependency on any particular claims/JWT library. On rejection: 429, JSON body,
// Retry-After: 86400 (matches the original notifications-api contract exactly).
func RequireQuota(q *Quota, featureKey string, claimsFn ClaimsFunc) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			scopeID, limit, ok := claimsFn(r.Context())
			if !ok {
				next.ServeHTTP(w, r)
				return
			}
			if limit == 0 {
				next.ServeHTTP(w, r)
				return
			}

			result, err := q.Check(r.Context(), scopeID, featureKey, limit)
			if err != nil {
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%d", result.Limit))
			w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%d", result.Remaining))
			w.Header().Set("X-RateLimit-Feature", result.Feature)

			if !result.Allowed {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "86400")
				w.WriteHeader(http.StatusTooManyRequests)
				json.NewEncoder(w).Encode(map[string]any{
					"error":       "usage_limit_reached",
					"feature":     result.Feature,
					"limit":       result.Limit,
					"used":        result.Used,
					"upgrade_url": q.UpgradeURL,
					"message":     fmt.Sprintf("Daily %s limit reached. Upgrade your plan or add overage.", featureKey),
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
