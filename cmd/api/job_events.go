package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/teryn-guzman/gatekeeper-asynchronous/internal/data"
)

const (
	sseHeartbeatInterval = 15 * time.Second
	// Each heartbeat resets http.Server.WriteTimeout before it expires.
	sseWriteDeadline = 25 * time.Second
)

type jobSnapshotReader interface {
	GetJobWithReads(context.Context, string) (*data.ImageJob, int, error)
}

// getJobEventsHandler streams full, authoritative job snapshots. Notifications
// only wake the handler; every payload is re-read from PostgreSQL.
func (app *application) getJobEventsHandler(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	jobID, ok := parseUUIDParam(jobID)
	if !ok {
		app.notFoundResponse(w, r)
		return
	}

	reader := app.sseJobReader
	if reader == nil {
		reader = app.models.Images
	}

	// Subscribe before the first read so a commit cannot land in a check/wait gap.
	wake, unsubscribe := app.jobNotifier.subscribe(jobID)
	defer unsubscribe()

	job, reads, err := reader.GetJobWithReads(r.Context(), jobID)
	if errors.Is(err, data.ErrRecordNotFound) {
		app.notFoundResponse(w, r)
		return
	}
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		app.serverErrorResponse(w, r, fmt.Errorf("SSE requires a streaming HTTP response"))
		return
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	startedAt := time.Now()
	streamID := app.sseStreamSeq.Add(1)
	active, peak := app.beginSSEStream()
	app.logger.Info("sse stream opened", "stream_id", streamID, "job_id", jobID, "active_streams", active, "peak_active_streams", peak)

	eventsSent := 0
	heartbeats := 0
	lastVersion := int64(-1)
	closeReason := "request_ended"
	defer func() {
		active = app.endSSEStream()
		app.logger.Info("sse stream closed",
			"stream_id", streamID,
			"job_id", jobID,
			"reason", closeReason,
			"duration_ms", time.Since(startedAt).Milliseconds(),
			"job_events_sent", eventsSent,
			"heartbeats", heartbeats,
			"database_reads", reads,
			"active_streams", active,
			"peak_active_streams", app.peakSSE.Load(),
		)
	}()

	heartbeat := time.NewTicker(sseHeartbeatInterval)
	defer heartbeat.Stop()

	if err := app.writeSSEJobUpdate(w, flusher, streamID, jobID, job); err != nil {
		closeReason = "write_error"
		return
	}
	lastVersion = job.Version
	eventsSent++
	app.logger.Info("sse job-update sent", "stream_id", streamID, "job_id", jobID, "version", job.Version, "status", job.Status)
	if isTerminalStatus(job.Status) {
		closeReason = "terminal"
		return
	}

	for {
		select {
		case <-wake:
			if r.Context().Err() != nil {
				closeReason = "client_disconnected"
				return
			}
			job, readCount, readErr := reader.GetJobWithReads(r.Context(), jobID)
			reads += readCount
			if readErr != nil {
				if r.Context().Err() != nil {
					closeReason = "client_disconnected"
					return
				}
				closeReason = "database_error"
				app.logger.Error("sse snapshot read failed", "stream_id", streamID, "job_id", jobID, "error", readErr)
				return
			}
			if job.Version <= lastVersion {
				continue
			}
			if err := app.writeSSEJobUpdate(w, flusher, streamID, jobID, job); err != nil {
				closeReason = "write_error"
				return
			}
			lastVersion = job.Version
			eventsSent++
			app.logger.Info("sse job-update sent", "stream_id", streamID, "job_id", jobID, "version", job.Version, "status", job.Status)
			if isTerminalStatus(job.Status) {
				closeReason = "terminal"
				return
			}
		case <-heartbeat.C:
			if err := app.writeSSEHeartbeat(w, flusher, streamID, jobID); err != nil {
				closeReason = "write_error"
				return
			}
			heartbeats++
			app.logger.Info("sse heartbeat", "stream_id", streamID, "job_id", jobID, "count", heartbeats)
		case <-app.shutdownCh:
			// A nil channel (tests that do not set one) blocks forever, so this
			// case only fires when the server has begun shutting down.
			closeReason = "server_shutdown"
			return
		case <-r.Context().Done():
			closeReason = "client_disconnected"
			return
		}
	}
}

// beginShutdown signals every open SSE handler to return. It is safe to call
// more than once. Ending a stream does not touch the worker or the job.
func (app *application) beginShutdown() {
	app.shutdownOnce.Do(func() {
		if app.shutdownCh != nil {
			close(app.shutdownCh)
		}
	})
}

func (app *application) beginSSEStream() (int64, int64) {
	app.sseMetricsMu.Lock()
	defer app.sseMetricsMu.Unlock()
	if app.activeSSE.Load() == 0 {
		app.peakSSE.Store(0)
	}
	active := app.activeSSE.Add(1)
	return active, app.updatePeakSSE(active)
}

func (app *application) endSSEStream() int64 {
	app.sseMetricsMu.Lock()
	defer app.sseMetricsMu.Unlock()
	return app.activeSSE.Add(-1)
}

func (app *application) updatePeakSSE(active int64) int64 {
	for {
		peak := app.peakSSE.Load()
		if active <= peak || app.peakSSE.CompareAndSwap(peak, active) {
			if active > peak {
				return active
			}
			return peak
		}
	}
}

func (app *application) writeSSEJobUpdate(w http.ResponseWriter, flusher http.Flusher, streamID uint64, jobID string, job *data.ImageJob) error {
	payload, err := json.Marshal(jobSnapshot(job))
	if err != nil {
		app.logger.Error("could not encode SSE job snapshot", "stream_id", streamID, "job_id", jobID, "error", err)
		return err
	}
	if err := resetSSEWriteDeadline(w); err != nil {
		app.logger.Error("could not set SSE write deadline", "stream_id", streamID, "job_id", jobID, "error", err)
		return err
	}
	if _, err := fmt.Fprintf(w, "event: job-update\ndata: %s\n\n", payload); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

func (app *application) writeSSEHeartbeat(w http.ResponseWriter, flusher http.Flusher, streamID uint64, jobID string) error {
	if err := resetSSEWriteDeadline(w); err != nil {
		app.logger.Error("could not set SSE heartbeat deadline", "stream_id", streamID, "job_id", jobID, "error", err)
		return err
	}
	if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

func resetSSEWriteDeadline(w http.ResponseWriter) error {
	controller := http.NewResponseController(w)
	err := controller.SetWriteDeadline(time.Now().Add(sseWriteDeadline))
	if errors.Is(err, http.ErrNotSupported) {
		// Test recorders and some custom ResponseWriters do not expose deadlines.
		return nil
	}
	return err
}
