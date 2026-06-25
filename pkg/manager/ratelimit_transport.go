package manager

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.uber.org/ratelimit"
)

const (
	// debridRateLimitRetries bounds transparent retries on a 429 from a debrid host.
	debridRateLimitRetries = 6
	// debridRateLimitBaseWait/MaxWait keep a 429 backoff short enough to clear TorBox's
	// per-minute window while staying under the download stall watchdog (90s of no
	// byte progress), so a rate-limit pause doesn't trip a stall-cancel.
	debridRateLimitBaseWait = 3 * time.Second
	debridRateLimitMaxWait  = 20 * time.Second
	// torboxRequestsPerMinute keeps the stream path (link-validation HEAD + requestdl
	// download GET) under TorBox's 300/min-per-token limit, smoothing the burst that a
	// bulk import otherwise creates when it resolves every file's link up front. The
	// API client has its own limiter + retries; the 429 retry below absorbs any overlap.
	torboxRequestsPerMinute = 200
)

// rateLimitTransport wraps a base RoundTripper to (a) smooth bursts of TorBox requests
// under its documented 300/min-per-token limit and (b) transparently retry HTTP 429
// responses with backoff (honoring Retry-After). Because the shared streamClient is used
// for BOTH the link-validation HEAD and the grab download GET, this turns a debrid rate
// limit into a brief pause instead of a failed download — which the *arr would otherwise
// blacklist as a false negative. Non-429 responses pass straight through.
type rateLimitTransport struct {
	base      http.RoundTripper
	torboxLim ratelimit.Limiter
}

func newRateLimitTransport(base http.RoundTripper) *rateLimitTransport {
	return &rateLimitTransport{
		base:      base,
		torboxLim: ratelimit.New(torboxRequestsPerMinute, ratelimit.Per(time.Minute)),
	}
}

func isTorboxHost(host string) bool {
	return strings.Contains(host, "torbox.app")
}

func (t *rateLimitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	torbox := isTorboxHost(req.URL.Host)
	for attempt := 0; ; attempt++ {
		if torbox && t.torboxLim != nil {
			t.torboxLim.Take()
		}
		resp, err := t.base.RoundTrip(req)
		if err != nil {
			return resp, err
		}
		// Only debrid download/link requests are GET/HEAD (no body), so the request is
		// safe to replay on a 429.
		if resp.StatusCode != http.StatusTooManyRequests || attempt >= debridRateLimitRetries {
			return resp, nil
		}
		wait := retryAfter(resp.Header)
		if wait <= 0 {
			wait = debridRateLimitBaseWait << uint(attempt)
			if wait > debridRateLimitMaxWait {
				wait = debridRateLimitMaxWait
			}
		}
		// Drain the small 429 body so the connection can be reused, then back off.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<12))
		_ = resp.Body.Close()
		timer := time.NewTimer(wait)
		select {
		case <-req.Context().Done():
			timer.Stop()
			return nil, req.Context().Err()
		case <-timer.C:
		}
	}
}

// retryAfter parses a Retry-After header (delta-seconds or HTTP date). TorBox does not
// document one, so this is best-effort; returns 0 when absent/unparseable.
func retryAfter(h http.Header) time.Duration {
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if when, err := http.ParseTime(v); err == nil {
		if d := time.Until(when); d > 0 {
			return d
		}
	}
	return 0
}
