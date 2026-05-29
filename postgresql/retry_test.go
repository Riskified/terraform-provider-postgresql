package postgresql

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/lib/pq"
)

func TestIsTransientError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"driver.ErrBadConn", driver.ErrBadConn, true},
		{"wrapped ErrBadConn", fmt.Errorf("query failed: %w", driver.ErrBadConn), true},
		{"io.EOF", io.EOF, true},
		{"net.OpError", &net.OpError{Op: "read", Err: errors.New("connection reset by peer")}, true},
		{"connection reset message", errors.New("read tcp 1.2.3.4:5->6.7.8.9:10: read: connection reset by peer"), true},
		{"connection timed out message", errors.New("read tcp 1.2.3.4:5->6.7.8.9:10: read: connection timed out"), true},
		{"broken pipe", errors.New("write tcp 1.2.3.4:5: broken pipe"), true},
		{"use of closed network connection", errors.New("use of closed network connection"), true},
		{"pq serialization_failure", &pq.Error{Code: "40001"}, true},
		{"pq connection exception class", &pq.Error{Code: "08006"}, true},
		{"pq crdb retry", &pq.Error{Code: "XXC00"}, true},
		{"plain syntax error", errors.New("syntax error at or near FOO"), false},
		{"pq undefined_table", &pq.Error{Code: "42P01"}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isTransientError(tc.err)
			if got != tc.want {
				t.Errorf("isTransientError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestRetryBackoffCap(t *testing.T) {
	base := 100 * time.Millisecond
	maxDelay := 5 * time.Second

	// With full jitter, the returned value is in [0, cap], where cap = min(2^(n-1)*base, maxDelay).
	// Verify the upper bound holds at every attempt and never overshoots maxDelay.
	for attempt := 1; attempt <= 20; attempt++ {
		for i := 0; i < 50; i++ {
			d := retryBackoff(attempt, base, maxDelay)
			if d < 0 {
				t.Fatalf("attempt %d: got negative delay %v", attempt, d)
			}
			if d > maxDelay {
				t.Fatalf("attempt %d: delay %v exceeded max %v", attempt, d, maxDelay)
			}
		}
	}
}

func TestRetryBackoffGrowsThenCaps(t *testing.T) {
	base := 10 * time.Millisecond
	maxDelay := 1 * time.Second

	// At attempt 1: ceiling = base = 10ms.
	// At attempt 7: ceiling = base * 2^6 = 640ms.
	// At attempt 8+: ceiling clamped to maxDelay = 1s.
	// Sanity-check that max-sampled values from many trials show growth and cap.
	maxObserved := func(attempt int) time.Duration {
		var m time.Duration
		for i := 0; i < 200; i++ {
			d := retryBackoff(attempt, base, maxDelay)
			if d > m {
				m = d
			}
		}
		return m
	}

	d1 := maxObserved(1)
	d4 := maxObserved(4)
	if d4 < d1 {
		t.Errorf("expected attempt 4 ceiling to be larger than attempt 1, got %v < %v", d4, d1)
	}
	d10 := maxObserved(10)
	if d10 > maxDelay {
		t.Errorf("attempt 10 sample %v exceeded cap %v", d10, maxDelay)
	}
}
