package middlewares

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/tech-engine/goscrapy/pkg/middlewaremanager"
)

const MIDDLEWARE_HTTP_RETRY_MAX_RETRIES = 3

// maxRetryBackoffShift caps the exponential backoff shift so that
// (1<<shift)*BaseDelay can never overflow time.Duration, even with the
// maximum of 255 retries.
const maxRetryBackoffShift = 20

var MIDDLEWARE_HTTP_RETRY_CODES = []uint16{500, 502, 503, 504, 522, 524, 408, 429}

// retryAttemptContextKey marks requests that are replayed attempts of the
// retry middleware, so that dedup-style middlewares registered *inside*
// Retry (e.g. DupeFilter) don't mistake a retry for a duplicate request.
type retryAttemptContextKey struct{}

type RetryCb func(*http.Request, uint8) bool

type RetryOpts struct {
	MaxRetries uint8
	Codes      []uint16
	BaseDelay  time.Duration
	Cb         RetryCb
}

func defaultRetryOpts() *RetryOpts {
	opts := &RetryOpts{
		MaxRetries: MIDDLEWARE_HTTP_RETRY_MAX_RETRIES,
		Codes:      MIDDLEWARE_HTTP_RETRY_CODES,
		BaseDelay:  1 * time.Second,
	}

	if value, ok := os.LookupEnv("MIDDLEWARE_HTTP_RETRY_MAX_RETRIES"); ok {
		if maxRetries, err := strconv.ParseUint(value, 10, 8); err == nil {
			opts.MaxRetries = uint8(maxRetries)
		}
	}

	if value, ok := os.LookupEnv("MIDDLEWARE_HTTP_RETRY_CODES"); ok {
		codesStr := strings.Split(value, ",")
		codes := make([]uint16, 0, len(codesStr))

		for _, codeStr := range codesStr {
			if code, err := strconv.ParseUint(strings.TrimSpace(codeStr), 10, 16); err == nil {
				codes = append(codes, uint16(code))
			}
		}

		if len(codes) > 0 {
			opts.Codes = codes[:]
		}

	}

	if value, ok := os.LookupEnv("MIDDLEWARE_HTTP_RETRY_BASE_DELAY"); ok {
		baseDelay, err := time.ParseDuration(value)
		if err == nil {
			opts.BaseDelay = baseDelay
		}
	}

	return opts
}

func Retry(opts ...RetryOpts) func(http.RoundTripper) http.RoundTripper {

	retryOpts := defaultRetryOpts()

	// overwrite defaults
	if len(opts) > 0 {
		if opts[0].MaxRetries > 0 {
			retryOpts.MaxRetries = opts[0].MaxRetries
		}

		if opts[0].Codes != nil {
			retryOpts.Codes = opts[0].Codes[:]
		}

		if opts[0].BaseDelay > 0 {
			retryOpts.BaseDelay = opts[0].BaseDelay
		}

		if opts[0].Cb != nil {
			retryOpts.Cb = opts[0].Cb
		}
	}

	return func(next http.RoundTripper) http.RoundTripper {
		return middlewaremanager.MiddlewareFunc(func(req *http.Request) (*http.Response, error) {

			// retries is kept in a wider int so that the maximum header
			// value (255) cannot wrap the attempt counter around to zero.
			retries := uint16(retryOpts.MaxRetries)

			retryHeader := req.Header.Get("X-Goscrapy-Middleware-Max-Retry")

			if retryHeader != "" {
				// values that don't parse as uint8 (incl. > 255) are
				// rejected and the configured default is kept.
				if r, err := strconv.ParseUint(retryHeader, 10, 8); err == nil {
					retries = uint16(r)
				}
				req.Header.Del("X-Goscrapy-Middleware-Max-Retry")
			}

			attempts := retries + 1

			var (
				resp *http.Response
				err  error
			)

			for i := uint16(0); i < attempts; i++ {
				if i > 0 {
					// mark replayed attempts so middlewares further down
					// the chain can tell them apart from new requests.
					req = req.WithContext(context.WithValue(req.Context(), retryAttemptContextKey{}, true))
				}

				resp, err = next.RoundTrip(req)

				// call retry callback, if present
				if i > 0 && retryOpts.Cb != nil && !retryOpts.Cb(req, uint8(i)) {
					break
				}

				// out of attempts: return whatever we got
				if i == attempts-1 {
					break
				}

				var retry bool
				switch {
				case errors.Is(err, ERR_DUPEFILTER_BLOCKED):
					// a duplicate blocked by the dupefilter is a permanent
					// failure: replaying it (with the retry-attempt marker
					// that bypasses the filter) would silently turn dedup
					// off, so it must never be retried.
					retry = false
				case err != nil:
					retry = true
				default:
					retry = slices.Contains(retryOpts.Codes, uint16(resp.StatusCode))
				}

				if !retry {
					break
				}

				// drain and close the abandoned response body so the
				// underlying connection can be reused instead of leaked.
				if resp != nil && resp.Body != nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}

				shift := i
				if shift > maxRetryBackoffShift {
					shift = maxRetryBackoffShift
				}

				timer := time.NewTimer(time.Duration(int64(1)<<shift) * retryOpts.BaseDelay)
				select {
				case <-timer.C:
				case <-req.Context().Done():
					timer.Stop()
					return nil, req.Context().Err()
				}
			}

			return resp, err
		})
	}
}
