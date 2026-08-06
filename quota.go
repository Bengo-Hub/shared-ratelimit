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
// Keys are bucketed by calendar day (UTC) and expire after ~25h — this package intentionally
// does not generalize the period; every known caller of this primitive is daily.
type Quota struct {
	redis *redis.Client
}

// NewQuota creates a new Redis-backed daily quota counter.
func NewQuota(rdb *redis.Client) *Quota {
	return &Quota{redis: rdb}
}

// QuotaResult is the result of a Check call.
type QuotaResult struct {
	Allowed   bool   `json:"allowed"`
	Feature   string `json:"feature"`
	Limit     int    `json:"limit"`
	Used      int    `json:"used"`
	Remaining int    `json:"remaining"`
}

// Check checks whether scopeID (typically a tenant ID) is within `limit` uses of `feature`
// today. A limit of -1 means unlimited. A limit of 0 means not configured (allow by default).
func (q *Quota) Check(ctx context.Context, scopeID, feature string, limit int) (*QuotaResult, error) {
	if limit < 0 {
		return &QuotaResult{Allowed: true, Feature: feature, Limit: -1, Used: 0, Remaining: -1}, nil
	}
	if limit == 0 {
		return &QuotaResult{Allowed: true, Feature: feature, Limit: 0, Used: 0, Remaining: 0}, nil
	}

	key := fmt.Sprintf("ratelimit:%s:%s:%s", scopeID, feature, time.Now().UTC().Format("2006-01-02"))

	count, err := q.redis.Incr(ctx, key).Result()
	if err != nil {
		return &QuotaResult{Allowed: true, Feature: feature, Limit: limit, Used: 0, Remaining: limit}, nil
	}

	if count == 1 {
		q.redis.Expire(ctx, key, 25*time.Hour)
	}

	used := int(count)
	remaining := limit - used
	if remaining < 0 {
		remaining = 0
	}

	if used > limit {
		q.redis.Decr(ctx, key)
		return &QuotaResult{Allowed: false, Feature: feature, Limit: limit, Used: limit, Remaining: 0}, nil
	}

	return &QuotaResult{Allowed: true, Feature: feature, Limit: limit, Used: used, Remaining: remaining}, nil
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
					"upgrade_url": "https://pricingapi.codevertexafrica.com/upgrade",
					"message":     fmt.Sprintf("Daily %s limit reached. Upgrade your plan or add overage.", featureKey),
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
