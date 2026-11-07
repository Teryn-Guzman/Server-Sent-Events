package main

import (
	"context"
	"database/sql"
	"errors"
	"image"
	_ "image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"time"

	"github.com/teryn-guzman/gatekeeper-asynchronous/internal/data"
)

// processNextImageJob claims one queued job, processes it, and records either
// a completed result or a durable failure for the polling endpoint.
func (app *application) processNextImageJob(ctx context.Context) (err error) {
	job, queueChanges, err := app.models.Images.ClaimNext(ctx)
	if err != nil {
		return err
	}
	app.logJobCommit(data.JobCommit{PublicID: job.PublicID, Version: job.Version, Status: job.Status, Stage: job.Stage, VariantsCompleted: job.VariantsCompleted, SimulationStep: job.SimulationStep}, "processing_started")
	for _, commit := range queueChanges {
		app.logJobCommit(commit, "queue_position_changed")
	}

	// A panic here (e.g. a malformed image tripping up resize/decode) must not
	// take down the whole server, and the already-claimed job must not be left
	// stuck in "processing" forever.
	defer func() {
		if r := recover(); r != nil {
			app.logger.Error("image worker panic", "job_id", job.PublicID, "panic", r)
			app.failImageJob(context.Background(), job.ID, "an unexpected error occurred while processing the image")
			err = nil
		}
	}()

	runtimeConfig := app.jobRuntimeConfig(job.PublicID)
	for _, delay := range []time.Duration{runtimeConfig.imageProcessingDelay, runtimeConfig.quietDelay} {
		if err := app.waitWorkerDelay(ctx, delay); err != nil {
			app.failImageJob(context.Background(), job.ID, "processing was interrupted")
			return err
		}
	}
	if err := app.runSimulatedProgress(ctx, job.ID, job.PublicID, runtimeConfig.simulationDuration, runtimeConfig.simulationInterval); err != nil {
		app.failImageJob(context.Background(), job.ID, "processing was interrupted")
		return err
	}

	imageRecord, err := app.models.Images.Original(ctx, job.ImageID)
	if err != nil {
		app.failImageJob(ctx, job.ID, "original image is unavailable")
		return err
	}

	source, err := os.Open(filepath.Join(app.config.storageDir, imageRecord.StoredFilename))
	if err != nil {
		app.failImageJob(ctx, job.ID, "original image is unavailable")
		return err
	}
	defer source.Close()

	decoded, _, err := image.Decode(source)
	if err != nil {
		app.failImageJob(ctx, job.ID, "original image could not be decoded")
		return err
	}

	profiles := []struct {
		name          string
		width, height int
		crop          bool
	}{
		{"thumbnail", 150, 150, true},
		{"preview", 800, 600, false},
		{"display", 1200, 900, false},
	}
	for index, profile := range profiles {
		output, width, height := resize(decoded, profile.width, profile.height, profile.crop)
		filename, err := randomFilename(".png")
		if err != nil {
			app.failImageJob(ctx, job.ID, "could not create output")
			return err
		}

		path := filepath.Join(app.variantStorageDir(), filename)
		file, err := os.Create(path)
		if err != nil {
			app.failImageJob(ctx, job.ID, "could not store output")
			return err
		}
		if err = png.Encode(file, output); err != nil {
			_ = file.Close()
			_ = os.Remove(path)
			app.failImageJob(ctx, job.ID, "could not encode output")
			return err
		}
		if err := file.Close(); err != nil {
			app.failImageJob(ctx, job.ID, "could not close output file")
			return err
		}

		info, err := os.Stat(path)
		if err != nil {
			app.failImageJob(ctx, job.ID, "could not read output metadata")
			return err
		}
		commit, err := app.models.Images.AddVariantAndProgress(ctx, job.ID, job.ImageID, profile.name, filename, width, height, info.Size(), index == len(profiles)-1)
		if err != nil {
			_ = os.Remove(path)
			app.failImageJob(ctx, job.ID, "could not record output")
			return err
		}
		app.logJobCommit(commit, "variant_progress")
	}
	return nil
}

func (app *application) waitWorkerDelay(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (app *application) runSimulatedProgress(ctx context.Context, jobID, publicID string, duration, interval time.Duration) error {
	if duration <= 0 || interval <= 0 {
		return nil
	}
	app.logger.Info("simulated progress phase started", "job_id", publicID, "duration", duration.String(), "interval", interval.String())
	deadline := time.Now().Add(duration)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if !time.Now().Before(deadline) {
				app.logger.Info("simulated progress phase finished", "job_id", publicID)
				return nil
			}
			commit, err := app.models.Images.AdvanceSimulation(ctx, jobID)
			if err != nil {
				return err
			}
			app.logJobCommit(commit, "simulated_progress")
		}
	}
}

func (app *application) logJobCommit(commit data.JobCommit, event string) {
	committedAt := time.Now()
	app.logger.Info("job committed",
		"event", event,
		"job_id", commit.PublicID,
		"version", commit.Version,
		"status", commit.Status,
		"stage", commit.Stage,
		"variants_completed", commit.VariantsCompleted,
		"simulation_step", commit.SimulationStep,
		"post_commit_at", committedAt.Format(time.RFC3339Nano),
	)
	if app.jobNotifier != nil {
		app.jobNotifier.publish(commit.PublicID)
	}
}

func (app *application) failImageJob(ctx context.Context, jobID, message string) {
	failCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		failCtx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
	}
	commit, err := app.models.Images.Fail(failCtx, jobID, message)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			app.logger.Error("could not persist image job failure", "error", err)
		}
		return
	}
	app.logJobCommit(commit, "failed")
}

// resize preserves aspect ratio, except for the fixed-size cropped thumbnail.
func resize(source image.Image, maxWidth, maxHeight int, crop bool) (image.Image, int, int) {
	bounds := source.Bounds()
	sw, sh := bounds.Dx(), bounds.Dy()
	scale := float64(maxWidth) / float64(sw)
	if value := float64(maxHeight) / float64(sh); (crop && value > scale) || (!crop && value < scale) {
		scale = value
	}
	width, height := int(float64(sw)*scale), int(float64(sh)*scale)
	if width < 1 {
		width = 1
	}
	if height < 1 {
		height = 1
	}
	if crop {
		width, height = maxWidth, maxHeight
	}

	output := image.NewRGBA(image.Rect(0, 0, width, height))
	scaledWidth, scaledHeight := int(float64(sw)*scale), int(float64(sh)*scale)
	offsetX, offsetY := 0, 0
	if crop {
		offsetX, offsetY = (scaledWidth-width)/2, (scaledHeight-height)/2
	}
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			sourceX := int(float64(x+offsetX) / scale)
			sourceY := int(float64(y+offsetY) / scale)
			output.Set(x, y, source.At(bounds.Min.X+sourceX, bounds.Min.Y+sourceY))
		}
	}
	return output, width, height
}

// startImageWorker runs one background worker that periodically polls the database for
// queued image jobs and processes them without blocking the HTTP request path.
func (app *application) startImageWorker(ctx context.Context) {
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		ticker := time.NewTicker(app.config.workerPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := app.processNextImageJob(ctx); err != nil && !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, context.Canceled) {
					app.logger.Error("image worker failed", "error", err)
				}
			}
		}
	}()
}
