package middlewares

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tech-engine/goscrapy/pkg/middlewaremanager"
)

// Regression: X-Goscrapy-Middleware-Max-Retry: 255 used to wrap the uint8
// attempt counter (255+1 == 0), so not a single request was sent yet the
// middleware returned (nil, nil).
func TestRetryHeaderMaxRetryOverflow(t *testing.T) {
	var hits atomic.Int32

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer testServer.Close()

	client := &http.Client{
		Transport: Retry(RetryOpts{
			MaxRetries: 1,
			BaseDelay:  time.Millisecond,
			Cb: func(r *http.Request, retry uint8) bool {
				// stop after 3 retries so we don't run all 256 attempts
				return retry < 3
			},
		})(http.DefaultTransport),
	}

	req, err := http.NewRequest("GET", testServer.URL, nil)
	require.NoError(t, err, "error creating http request")
	req.Header.Set("X-Goscrapy-Middleware-Max-Retry", "255")

	resp, err := client.Do(req)
	require.NoError(t, err, "error making request")
	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	// 1 initial attempt + 3 retries must actually hit the network.
	assert.Equal(t, int32(4), hits.Load(), "requests never reached the network: retry counter wrapped")
}

// An unparseable or out-of-range header value must be rejected and the
// configured default kept.
func TestRetryHeaderInvalidValueFallsBack(t *testing.T) {
	for _, headerValue := range []string{"999", "abc", "-1"} {
		t.Run(headerValue, func(t *testing.T) {
			var hits atomic.Int32

			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer testServer.Close()

			client := &http.Client{
				Transport: Retry(RetryOpts{
					MaxRetries: 2,
					BaseDelay:  time.Millisecond,
				})(http.DefaultTransport),
			}

			req, err := http.NewRequest("GET", testServer.URL, nil)
			require.NoError(t, err, "error creating http request")
			req.Header.Set("X-Goscrapy-Middleware-Max-Retry", headerValue)

			resp, err := client.Do(req)
			require.NoError(t, err, "error making request")
			defer resp.Body.Close()

			assert.Equal(t, int32(3), hits.Load(), "invalid header must fall back to configured MaxRetries")
		})
	}
}

// Regression: response bodies abandoned on 5xx/429 retries were never
// drained and closed, exhausting the connection pool. With the body
// drained, the next attempt must reuse the same connection.
func TestRetryDrainsBodyAndReusesConnection(t *testing.T) {
	var hits atomic.Int32

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("try again later"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()

	var reused atomic.Bool
	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Reused {
				reused.Store(true)
			}
		},
	}

	client := &http.Client{
		Transport: Retry(RetryOpts{
			MaxRetries: 2,
			BaseDelay:  time.Millisecond,
		})(http.DefaultTransport),
	}

	req, err := http.NewRequest("GET", testServer.URL, nil)
	require.NoError(t, err, "error creating http request")
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	resp, err := client.Do(req)
	require.NoError(t, err, "error making request")
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(2), hits.Load())
	assert.True(t, reused.Load(), "expected the connection of the abandoned 503 response to be reused")
}

// Regression: with DupeFilter registered before Retry, every attempt after
// the first was blocked as a duplicate and retries never reached the
// network. Retries must work with either registration order.
func TestRetryWithDupeFilterRegistrationOrders(t *testing.T) {
	registrations := map[string]func(m *middlewaremanager.MiddlewareManager){
		"retry registered first": func(m *middlewaremanager.MiddlewareManager) {
			m.Add(
				Retry(RetryOpts{MaxRetries: 3, BaseDelay: time.Millisecond}),
				DupeFilter(),
			)
		},
		"dupefilter registered first": func(m *middlewaremanager.MiddlewareManager) {
			m.Add(
				DupeFilter(),
				Retry(RetryOpts{MaxRetries: 3, BaseDelay: time.Millisecond}),
			)
		},
	}

	for name, register := range registrations {
		t.Run(name, func(t *testing.T) {
			var hits atomic.Int32

			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if hits.Add(1) < 3 {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer testServer.Close()

			client := &http.Client{Transport: http.DefaultTransport}
			register(middlewaremanager.New(client))

			req, err := http.NewRequest("GET", testServer.URL, nil)
			require.NoError(t, err, "error creating http request")

			resp, err := client.Do(req)
			require.NoError(t, err, "error making request")
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, int32(3), hits.Load(), "retries must reach the network regardless of middleware registration order")
		})
	}
}

// Retrying must not weaken deduplication of genuinely new requests: the
// first attempt is still fingerprinted, so a real duplicate is blocked.
func TestRetryWithDupeFilterStillBlocksDuplicates(t *testing.T) {
	var hits atomic.Int32

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()

	client := &http.Client{Transport: http.DefaultTransport}
	m := middlewaremanager.New(client)
	m.Add(
		DupeFilter(),
		Retry(RetryOpts{MaxRetries: 2, BaseDelay: time.Millisecond}),
	)

	for i := 0; i < 2; i++ {
		req, err := http.NewRequest("GET", testServer.URL, nil)
		require.NoError(t, err, "error creating http request")

		resp, err := client.Do(req)
		if i == 0 {
			require.NoError(t, err, "error making request")
			resp.Body.Close()
			continue
		}

		assert.Nil(t, resp)
		assert.ErrorIs(t, err, ERR_DUPEFILTER_BLOCKED, "second identical request must be blocked")
	}

	assert.Equal(t, int32(1), hits.Load())
}

// The backoff wait between attempts must respect context cancellation.
func TestRetryRespectsContextCancellation(t *testing.T) {
	var hits atomic.Int32

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer testServer.Close()

	client := &http.Client{
		Transport: Retry(RetryOpts{
			MaxRetries: 5,
			BaseDelay:  5 * time.Second,
		})(http.DefaultTransport),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", testServer.URL, nil)
	require.NoError(t, err, "error creating http request")

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(start)

	assert.Nil(t, resp)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, elapsed, 2*time.Second, "retry backoff must respect context cancellation")
	assert.Equal(t, int32(1), hits.Load(), "no further attempt must be sent after cancellation")
}
