package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/teryn-guzman/gatekeeper-asynchronous/internal/data"
)

const testSSEJobID = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"

type sseTestReader struct {
	mu        sync.Mutex
	job       *data.ImageJob
	err       error
	reads     int
	afterRead func()
}

func (r *sseTestReader) GetJobWithReads(_ context.Context, _ string) (*data.ImageJob, int, error) {
	r.mu.Lock()
	r.reads++
	if r.err != nil {
		r.mu.Unlock()
		return nil, 1, r.err
	}
	job := *r.job
	job.Variants = append([]data.Variant(nil), r.job.Variants...)
	afterRead := r.afterRead
	r.afterRead = nil
	r.mu.Unlock()
	if afterRead != nil {
		afterRead()
	}
	return &job, 1, nil
}

func (r *sseTestReader) setJob(job *data.ImageJob) {
	r.mu.Lock()
	r.job = job
	r.mu.Unlock()
}

type sseTestWriter struct {
	header  http.Header
	mu      sync.Mutex
	status  int
	body    strings.Builder
	flushes chan struct{}
}

func newSSETestWriter() *sseTestWriter {
	return &sseTestWriter{header: make(http.Header), flushes: make(chan struct{}, 8)}
}

func (w *sseTestWriter) Header() http.Header { return w.header }

func (w *sseTestWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = status
	}
}

func (w *sseTestWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(p)
}

func (w *sseTestWriter) Flush() {
	select {
	case w.flushes <- struct{}{}:
	default:
	}
}

func (w *sseTestWriter) result() (int, string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.status, w.body.String()
}

func testSSEApp(reader jobSnapshotReader) *application {
	return &application{
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		jobNotifier:  newJobNotifier(),
		sseJobReader: reader,
	}
}

func TestJobEventsReturns404BeforeStartingUnknownStream(t *testing.T) {
	reader := &sseTestReader{err: data.ErrRecordNotFound}
	app := testSSEApp(reader)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+testSSEJobID+"/events", nil)
	request.SetPathValue("id", testSSEJobID)

	app.getJobEventsHandler(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("unknown job began an SSE stream with Content-Type %q", got)
	}
	if got := len(app.jobNotifier.subscribers[testSSEJobID]); got != 0 {
		t.Fatalf("unknown job left %d notifier subscriptions", got)
	}
}

func TestJobEventsSendsInitialAndTerminalSnapshotsWithoutEventIDs(t *testing.T) {
	reader := &sseTestReader{job: &data.ImageJob{
		PublicID: testSSEJobID, Status: "processing", Version: 2, Stage: "starting", VariantsCompleted: 0,
	}}
	app := testSSEApp(reader)
	writer := newSSETestWriter()
	request := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+testSSEJobID+"/events", nil)
	request.SetPathValue("id", testSSEJobID)
	done := make(chan struct{})
	go func() {
		app.getJobEventsHandler(writer, request)
		close(done)
	}()

	waitSSEFlush(t, writer)
	reader.setJob(&data.ImageJob{
		PublicID: testSSEJobID, Status: "completed", Version: 3, Stage: "display_ready", VariantsCompleted: 3,
		Variants: []data.Variant{
			{Name: "thumbnail", Width: 150, Height: 150},
			{Name: "preview", Width: 800, Height: 600},
			{Name: "display", Width: 1200, Height: 900},
		},
	})
	app.jobNotifier.publish(testSSEJobID)
	waitSSEFlush(t, writer)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("terminal SSE stream did not close")
	}

	status, body := writer.result()
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if got := writer.Header().Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := writer.Header().Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("Cache-Control = %q", got)
	}
	if got := writer.Header().Get("X-Accel-Buffering"); got != "no" {
		t.Fatalf("X-Accel-Buffering = %q", got)
	}
	if strings.Contains(body, "id:") {
		t.Fatalf("SSE frame must not include an id field: %s", body)
	}
	if strings.Count(body, "event: job-update\n") != 2 {
		t.Fatalf("got body with unexpected event count: %s", body)
	}
	if !strings.HasSuffix(body, "\n\n") {
		t.Fatalf("SSE event frames must end with a blank line: %q", body)
	}
	for _, fragment := range []string{`"version":2`, `"version":3`, `"status":"completed"`, `"variants_completed":3`, `"name":"thumbnail"`, `"name":"preview"`, `"name":"display"`} {
		if !strings.Contains(body, fragment) {
			t.Errorf("SSE body missing %q: %s", fragment, body)
		}
	}
	if len(app.jobNotifier.subscribers[testSSEJobID]) != 0 {
		t.Fatal("terminal stream did not unsubscribe")
	}
}

func TestJobEventsDoesNotMissCommitDuringInitialRead(t *testing.T) {
	app := testSSEApp(nil)
	reader := &sseTestReader{job: &data.ImageJob{PublicID: testSSEJobID, Status: "processing", Version: 2}}
	reader.afterRead = func() {
		reader.setJob(&data.ImageJob{
			PublicID: testSSEJobID, Status: "completed", Version: 3, Stage: "display_ready", VariantsCompleted: 3,
			Variants: []data.Variant{{Name: "thumbnail"}, {Name: "preview"}, {Name: "display"}},
		})
		app.jobNotifier.publish(testSSEJobID)
	}
	app.sseJobReader = reader
	writer := newSSETestWriter()
	request := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+testSSEJobID+"/events", nil)
	request.SetPathValue("id", testSSEJobID)
	done := make(chan struct{})
	go func() {
		app.getJobEventsHandler(writer, request)
		close(done)
	}()

	waitSSEFlush(t, writer)
	waitSSEFlush(t, writer)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stream did not deliver the commit racing with the initial read")
	}
	_, body := writer.result()
	if !strings.Contains(body, `"version":2`) || !strings.Contains(body, `"version":3`) {
		t.Fatalf("expected the initial and raced committed snapshots, got: %s", body)
	}
}

func TestJobEventsBroadcastsUpdateToMultipleObservers(t *testing.T) {
	reader := &sseTestReader{job: &data.ImageJob{PublicID: testSSEJobID, Status: "processing", Version: 2}}
	app := testSSEApp(reader)
	writers := []*sseTestWriter{newSSETestWriter(), newSSETestWriter()}
	done := make([]chan struct{}, len(writers))
	for index, writer := range writers {
		request := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+testSSEJobID+"/events", nil)
		request.SetPathValue("id", testSSEJobID)
		done[index] = make(chan struct{})
		go func(writer http.ResponseWriter, request *http.Request, finished chan struct{}) {
			app.getJobEventsHandler(writer, request)
			close(finished)
		}(writer, request, done[index])
		waitSSEFlush(t, writer)
	}
	if got := app.activeSSE.Load(); got != 2 {
		t.Fatalf("active SSE count = %d, want 2 observers", got)
	}
	if got := app.peakSSE.Load(); got != 2 {
		t.Fatalf("peak SSE count = %d, want 2 observers", got)
	}
	reader.setJob(&data.ImageJob{
		PublicID: testSSEJobID, Status: "failed", Version: 3, Stage: "thumbnail_ready", Error: stringPointer("the image could not be processed"),
	})
	app.jobNotifier.publish(testSSEJobID)
	for index, writer := range writers {
		waitSSEFlush(t, writer)
		select {
		case <-done[index]:
		case <-time.After(time.Second):
			t.Fatalf("observer %d did not close after failed terminal update", index)
		}
		_, body := writer.result()
		if !strings.Contains(body, `"status":"failed"`) || !strings.Contains(body, `"error":"the image could not be processed"`) {
			t.Errorf("observer %d did not receive the safe failed snapshot: %s", index, body)
		}
	}
}

func TestSSEHeartbeatIsACommentFrame(t *testing.T) {
	app := testSSEApp(nil)
	writer := newSSETestWriter()
	if err := app.writeSSEHeartbeat(writer, writer, 1, testSSEJobID); err != nil {
		t.Fatalf("writeSSEHeartbeat() error = %v", err)
	}
	_, body := writer.result()
	if body != ": heartbeat\n\n" {
		t.Fatalf("heartbeat body = %q, want comment frame", body)
	}
	select {
	case <-writer.flushes:
	default:
		t.Fatal("heartbeat was not flushed")
	}
}

func TestJobEventsCancellationUnsubscribesObserver(t *testing.T) {
	reader := &sseTestReader{job: &data.ImageJob{PublicID: testSSEJobID, Status: "processing", Version: 1}}
	app := testSSEApp(reader)
	writer := newSSETestWriter()
	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+testSSEJobID+"/events", nil).WithContext(ctx)
	request.SetPathValue("id", testSSEJobID)
	done := make(chan struct{})
	go func() {
		app.getJobEventsHandler(writer, request)
		close(done)
	}()
	waitSSEFlush(t, writer)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SSE handler did not stop after request cancellation")
	}
	if len(app.jobNotifier.subscribers[testSSEJobID]) != 0 {
		t.Fatal("cancelled stream did not unsubscribe")
	}
	if app.activeSSE.Load() != 0 {
		t.Fatalf("active SSE count = %d after close, want 0", app.activeSSE.Load())
	}
}

func TestJobEventsServerShutdownEndsOpenStreams(t *testing.T) {
	reader := &sseTestReader{job: &data.ImageJob{PublicID: testSSEJobID, Status: "processing", Version: 1}}
	app := testSSEApp(reader)
	app.shutdownCh = make(chan struct{})
	writers := []*sseTestWriter{newSSETestWriter(), newSSETestWriter()}
	done := make([]chan struct{}, len(writers))
	for index, writer := range writers {
		request := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+testSSEJobID+"/events", nil)
		request.SetPathValue("id", testSSEJobID)
		done[index] = make(chan struct{})
		go func(writer http.ResponseWriter, request *http.Request, finished chan struct{}) {
			app.getJobEventsHandler(writer, request)
			close(finished)
		}(writer, request, done[index])
		waitSSEFlush(t, writer)
	}

	app.beginShutdown()
	app.beginShutdown() // a second call must not panic on a closed channel

	for index := range writers {
		select {
		case <-done[index]:
		case <-time.After(time.Second):
			t.Fatalf("observer %d stayed open after server shutdown began", index)
		}
	}
	if len(app.jobNotifier.subscribers[testSSEJobID]) != 0 {
		t.Fatal("shutdown did not unsubscribe observers")
	}
	if app.activeSSE.Load() != 0 {
		t.Fatalf("active SSE count = %d after shutdown, want 0", app.activeSSE.Load())
	}
	// Stopping observation must not alter the job itself.
	if reader.job.Status != "processing" || reader.job.Version != 1 {
		t.Fatalf("shutdown changed the job: status=%q version=%d", reader.job.Status, reader.job.Version)
	}
}

func TestJobEventsDatabaseErrorBeforeHeadersUsesJSONError(t *testing.T) {
	reader := &sseTestReader{err: errors.New("database unavailable")}
	app := testSSEApp(reader)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+testSSEJobID+"/events", nil)
	request.SetPathValue("id", testSSEJobID)

	app.getJobEventsHandler(recorder, request)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
}

func TestJobEventsSkipsUnchangedVersionsAndCoalescesGaps(t *testing.T) {
	reader := &sseTestReader{job: &data.ImageJob{PublicID: testSSEJobID, Status: "processing", Version: 2, Stage: "starting"}}
	app := testSSEApp(reader)
	writer := newSSETestWriter()
	request := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+testSSEJobID+"/events", nil)
	request.SetPathValue("id", testSSEJobID)
	done := make(chan struct{})
	go func() {
		app.getJobEventsHandler(writer, request)
		close(done)
	}()
	waitSSEFlush(t, writer)

	// A wake-up whose re-read shows no newer persisted version must not send an event.
	app.jobNotifier.publish(testSSEJobID)
	waitSSEReads(t, reader, 2)
	time.Sleep(20 * time.Millisecond)
	if _, body := writer.result(); strings.Count(body, "event: job-update\n") != 1 {
		t.Fatalf("an unchanged version produced an extra event: %s", body)
	}

	// Versions 3 and 4 are never observed by this stream. It must still send the
	// current snapshot, including the terminal version, rather than a replay.
	reader.setJob(&data.ImageJob{
		PublicID: testSSEJobID, Status: "completed", Version: 5, Stage: "display_ready", VariantsCompleted: 3,
		Variants: []data.Variant{{Name: "thumbnail"}, {Name: "preview"}, {Name: "display"}},
	})
	app.jobNotifier.publish(testSSEJobID)
	waitSSEFlush(t, writer)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stream did not close after the terminal snapshot")
	}
	_, body := writer.result()
	if strings.Count(body, "event: job-update\n") != 2 {
		t.Fatalf("expected the initial and terminal snapshots only: %s", body)
	}
	if !strings.Contains(body, `"version":5`) {
		t.Fatalf("terminal snapshot missing: %s", body)
	}
	if strings.Contains(body, `"version":3`) || strings.Contains(body, `"version":4`) {
		t.Fatalf("skipped versions must not be replayed: %s", body)
	}
}

func waitSSEReads(t *testing.T, reader *sseTestReader, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		reader.mu.Lock()
		reads := reader.reads
		reader.mu.Unlock()
		if reads >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("handler did not re-read the job after the wake-up (want %d reads)", want)
}

func waitSSEFlush(t *testing.T, writer *sseTestWriter) {
	t.Helper()
	select {
	case <-writer.flushes:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for SSE flush")
	}
}

func stringPointer(value string) *string { return &value }
