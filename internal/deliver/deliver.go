// Package deliver holds the pure delivery-policy helpers: HMAC signing
// of payloads and the exponential backoff schedule.
package deliver

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// Sign returns the value for the X-Relayhook-Signature header:
// "sha256=" followed by the hex-encoded HMAC-SHA256 of the raw payload.
func Sign(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// Backoff returns the delay before the next attempt after attempt n
// (1-based) has failed: base * 2^(n-1). Jitter is added by the caller.
func Backoff(attempt int, base time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if base < 0 {
		base = 0
	}
	return base << (attempt - 1)
}
