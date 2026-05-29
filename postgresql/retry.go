package postgresql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/lib/pq"
)

const (
	defaultMaxRetries     = 5
	defaultRetryBaseDelay = 100 * time.Millisecond
	defaultRetryMaxDelay  = 30 * time.Second
)

// isTransientError reports whether err looks like a recoverable network or
// CockroachDB-side retry signal. The CRDB cluster behind a load balancer is the
// primary source of these errors during long refresh phases — connections get
// reset by peer, idle-reaped, or hit serialization retry conditions.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return true
		}
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}

	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		// CRDB serialization / retry codes plus connection-failure class.
		switch pqErr.Code.Class() {
		case "08", // connection exception
			"57": // operator intervention (e.g. admin shutdown)
			return true
		}
		switch string(pqErr.Code) {
		case "40001", // serialization_failure
			"40003", // statement_completion_unknown
			"XXC00": // crdb retry txn
			return true
		}
	}

	msg := err.Error()
	if strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "connection timed out") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "bad connection") ||
		strings.Contains(msg, "EOF") {
		return true
	}

	return false
}

// retryBackoff returns the sleep duration before attempt n (1-indexed),
// using exponential backoff with full jitter, capped at maxDelay.
func retryBackoff(attempt int, base, maxDelay time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	// 2^(attempt-1) * base, capped.
	d := base << uint(attempt-1)
	if d <= 0 || d > maxDelay {
		d = maxDelay
	}
	// Full jitter: random in [0, d].
	return time.Duration(rand.Int63n(int64(d) + 1))
}

// retry runs fn up to maxRetries+1 times, sleeping between attempts on
// transient errors. Non-transient errors return immediately.
func (db *DBConnection) retry(label string, fn func() error) error {
	maxRetries := db.client.config.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	maxDelay := time.Duration(db.client.config.RetryMaxDelayMs) * time.Millisecond
	if maxDelay <= 0 {
		maxDelay = defaultRetryMaxDelay
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if !isTransientError(err) {
			return err
		}
		lastErr = err
		if attempt == maxRetries {
			break
		}
		sleep := retryBackoff(attempt+1, defaultRetryBaseDelay, maxDelay)
		log.Printf("[WARN] %s: transient error (attempt %d/%d), retrying in %s: %v",
			label, attempt+1, maxRetries+1, sleep, err)
		time.Sleep(sleep)
	}
	return lastErr
}

// QueryRetry wraps db.Query with transient-error retry.
func (db *DBConnection) QueryRetry(query string, args ...interface{}) (*sql.Rows, error) {
	var rows *sql.Rows
	err := db.retry("Query", func() error {
		r, qerr := db.DB.Query(query, args...)
		if qerr != nil {
			return qerr
		}
		rows = r
		return nil
	})
	return rows, err
}

// QueryRowRetry wraps db.QueryRow. Because lib/pq defers query execution until
// Scan(), we run the actual query+scan inside the retry by accepting a scan
// closure. The closure must populate the destination from the row.
func (db *DBConnection) QueryRowRetry(scan func(*sql.Row) error, query string, args ...interface{}) error {
	return db.retry("QueryRow", func() error {
		return scan(db.DB.QueryRow(query, args...))
	})
}

// ExecRetry wraps db.Exec with transient-error retry. Callers must ensure the
// statement is safe to retry (idempotent). Pre-execution transient failures
// (connect-time / ErrBadConn) are always safe; for mid-statement failures the
// retry assumes the caller knows the statement is replay-safe.
func (db *DBConnection) ExecRetry(query string, args ...interface{}) (sql.Result, error) {
	var res sql.Result
	err := db.retry("Exec", func() error {
		r, eerr := db.DB.Exec(query, args...)
		if eerr != nil {
			return eerr
		}
		res = r
		return nil
	})
	return res, err
}
