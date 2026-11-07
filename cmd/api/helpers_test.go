package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseUUIDParam(t *testing.T) {
	const canonical = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"
	tests := []struct {
		name   string
		input  string
		want   string
		wantOK bool
	}{
		{"canonical", canonical, canonical, true},
		{"uppercase is normalised", "0190A1B2-C3D4-7E5F-8A9B-0C1D2E3F4A5B", canonical, true},
		{"not a uuid", "abc", "", false},
		{"no hyphens", "0190a1b2c3d47e5f8a9b0c1d2e3f4a5b", "", false},
		{"trailing junk", canonical + "x", "", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := parseUUIDParam(test.input)
			if ok != test.wantOK || (ok && got != test.want) {
				t.Fatalf("parseUUIDParam(%q) = %q, %v; want %q, %v", test.input, got, ok, test.want, test.wantOK)
			}
		})
	}
}

// These requests must be answered before any database or notifier work, so the
// application can be built without models or a notifier.
func TestHandlersReturn404ForMalformedIDs(t *testing.T) {
	app := &application{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/jobs/{id}", app.getJobHandler)
	mux.HandleFunc("GET /v1/jobs/{id}/events", app.getJobEventsHandler)
	mux.HandleFunc("GET /v1/images/{id}/variants/{name}", app.getVariantHandler)

	for _, path := range []string{
		"/v1/jobs/abc",
		"/v1/jobs/abc?after=1&wait=2",
		"/v1/jobs/abc/events",
		"/v1/images/abc/variants/thumbnail",
	} {
		t.Run(path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
			if recorder.Code != http.StatusNotFound {
				t.Fatalf("GET %s = %d, want 404", path, recorder.Code)
			}
		})
	}
}
