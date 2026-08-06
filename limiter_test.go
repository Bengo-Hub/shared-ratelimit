package ratelimit

import (
	"net/http"
	"testing"
)

func TestIPKey(t *testing.T) {
	cases := []struct {
		name string
		req  func() *http.Request
		want string
	}{
		{
			name: "x-forwarded-for takes first entry",
			req: func() *http.Request {
				r, _ := http.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("X-Forwarded-For", "203.0.113.5, 10.0.0.1")
				return r
			},
			want: "ip:203.0.113.5",
		},
		{
			name: "x-real-ip fallback",
			req: func() *http.Request {
				r, _ := http.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("X-Real-IP", "198.51.100.7")
				return r
			},
			want: "ip:198.51.100.7",
		},
		{
			name: "remote addr strips port",
			req: func() *http.Request {
				r, _ := http.NewRequest(http.MethodGet, "/", nil)
				r.RemoteAddr = "192.0.2.1:54321"
				return r
			},
			want: "ip:192.0.2.1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IPKey(tc.req()); got != tc.want {
				t.Errorf("IPKey() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTenantKey(t *testing.T) {
	keyFn := TenantKey("X-Tenant-ID")

	r, _ := http.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("X-Tenant-ID", "acme")
	if got := keyFn(r); got != "tenant:acme" {
		t.Errorf("TenantKey() = %q, want %q", got, "tenant:acme")
	}

	r2, _ := http.NewRequest(http.MethodGet, "/", nil)
	r2.RemoteAddr = "192.0.2.1:1234"
	if got := keyFn(r2); got != "ip:192.0.2.1" {
		t.Errorf("TenantKey() fallback = %q, want %q", got, "ip:192.0.2.1")
	}
}
