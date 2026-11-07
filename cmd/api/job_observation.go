package main

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/teryn-guzman/gatekeeper-asynchronous/internal/data"
)

const defaultHoldingSeconds = 20

type observationRequest struct {
	after int64
	wait  time.Duration
}

func parseObservationRequest(r *http.Request) (*observationRequest, error) {
	query := r.URL.Query()
	for key := range query {
		if key != "after" && key != "wait" {
			return nil, errors.New("only after and wait query parameters are supported")
		}
	}
	afterValues, hasAfter := query["after"]
	waitValues, hasWait := query["wait"]
	if !hasAfter {
		if hasWait {
			return nil, errors.New("wait requires after")
		}
		return nil, nil
	}
	if len(afterValues) != 1 {
		return nil, errors.New("after must be supplied once")
	}
	after, err := strconv.ParseInt(afterValues[0], 10, 64)
	if err != nil || after < 0 {
		return nil, errors.New("after must be a nonnegative integer")
	}
	waitSeconds := defaultHoldingSeconds
	if hasWait {
		if len(waitValues) != 1 {
			return nil, errors.New("wait must be supplied once")
		}
		waitSeconds, err = strconv.Atoi(waitValues[0])
		if err != nil || waitSeconds < 1 || waitSeconds > defaultHoldingSeconds {
			return nil, errors.New("wait must be an integer from 1 to 20 seconds")
		}
	}
	return &observationRequest{after: after, wait: time.Duration(waitSeconds) * time.Second}, nil
}

func isTerminalStatus(status string) bool {
	return status == "completed" || status == "failed"
}

// getJobHandler serves prompt snapshots and version-based held observations.
// It subscribes before reading so a commit cannot land in the check/wait gap.
func (app *application) getJobHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	databaseReads := 0
	jobID := r.PathValue("id")
	defer func() {
		app.logger.Info("observation database reads", "job_id", jobID, "queries", databaseReads)
	}()
	observation, err := parseObservationRequest(r)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}
	jobID, ok := parseUUIDParam(jobID)
	if !ok {
		app.notFoundResponse(w, r)
		return
	}
	if observation == nil {
		job, reads, err := app.models.Images.GetJobWithReads(r.Context(), jobID)
		databaseReads += reads
		if errors.Is(err, data.ErrRecordNotFound) {
			app.notFoundResponse(w, r)
			return
		}
		if err != nil {
			app.serverErrorResponse(w, r, err)
			return
		}
		app.writeJobSnapshot(w, job)
		return
	}

	wake, unsubscribe := app.jobNotifier.subscribe(jobID)
	defer unsubscribe()
	timer := time.NewTimer(observation.wait)
	defer timer.Stop()

	for {
		job, reads, err := app.models.Images.GetJobWithReads(r.Context(), jobID)
		databaseReads += reads
		if errors.Is(err, data.ErrRecordNotFound) {
			app.notFoundResponse(w, r)
			return
		}
		if err != nil {
			app.serverErrorResponse(w, r, err)
			return
		}
		if job.Version < observation.after {
			app.badRequestResponse(w, r, errors.New("after exceeds the current job version"))
			return
		}
		if isTerminalStatus(job.Status) || job.Version > observation.after {
			app.writeJobSnapshot(w, job)
			return
		}

		select {
		case <-wake:
			// Signals only indicate that persisted state may have changed.
			continue
		case <-timer.C:
			// The loop's final authoritative read determines 200 versus 204.
			var reads int
			job, reads, err = app.models.Images.GetJobWithReads(r.Context(), jobID)
			databaseReads += reads
			if errors.Is(err, data.ErrRecordNotFound) {
				app.notFoundResponse(w, r)
				return
			}
			if err != nil {
				app.serverErrorResponse(w, r, err)
				return
			}
			if job.Version < observation.after {
				app.badRequestResponse(w, r, errors.New("after exceeds the current job version"))
				return
			}
			if isTerminalStatus(job.Status) || job.Version > observation.after {
				app.writeJobSnapshot(w, job)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		case <-r.Context().Done():
			// Cancellation ends only this observation; it does not touch the worker.
			return
		}
	}
}

func (app *application) writeJobSnapshot(w http.ResponseWriter, job *data.ImageJob) {
	_ = app.writeJSON(w, http.StatusOK, jobSnapshot(job), nil)
}

func jobSnapshot(job *data.ImageJob) envelope {
	return envelope{
		"id":                 job.PublicID,
		"image_id":           job.ImageID,
		"status":             job.Status,
		"queue_position":     job.QueuePosition,
		"queued_at":          job.QueuedAt,
		"started_at":         job.StartedAt,
		"completed_at":       job.CompletedAt,
		"failed_at":          job.FailedAt,
		"error":              job.Error,
		"version":            job.Version,
		"stage":              job.Stage,
		"variants_completed": job.VariantsCompleted,
		"simulation_step":    job.SimulationStep,
		"variants":           job.Variants,
	}
}
