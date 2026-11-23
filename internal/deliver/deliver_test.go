package deliver

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	base := 2 * time.Second
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 32 * time.Second},
	}
	for _, c := range cases {
		if got := Backoff(c.attempt, base); got != c.want {
			t.Errorf("Backoff(%d, %s) = %s, want %s", c.attempt, base, got, c.want)
		}
	}
}

func TestBackoffEdgeCases(t *testing.T) {
	if got := Backoff(0, time.Second); got != time.Second {
		t.Errorf("Backoff(0, 1s) = %s, want 1s (attempt clamped to 1)", got)
	}
	if got := Backoff(3, 0); got != 0 {
		t.Errorf("Backoff(3, 0) = %s, want 0", got)
	}
}

func TestSignKnownVector(t *testing.T) {
	// Well-known HMAC-SHA256 test vector: key "key",
	// message "The quick brown fox jumps over the lazy dog".
	got := Sign("key", []byte("The quick brown fox jumps over the lazy dog"))
	want := "sha256=f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
	if got != want {
		t.Errorf("Sign() = %s, want %s", got, want)
	}
}

func TestSignDependsOnSecretAndPayload(t *testing.T) {
	a := Sign("secret-a", []byte(`{"x":1}`))
	b := Sign("secret-b", []byte(`{"x":1}`))
	c := Sign("secret-a", []byte(`{"x":2}`))
	if a == b || a == c {
		t.Error("signature must change with both secret and payload")
	}
}
