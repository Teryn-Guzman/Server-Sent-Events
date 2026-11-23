package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/teryn-guzman/gatekeeper-asynchronous/internal/data"
)

// The server is the authority for upload limits, storage, and durable job creation.
// Decode validation happens in the worker so corrupt image jobs are observable.
const (
	maxImageSize   int64 = 10 * 1024 * 1024
	maxRequestSize int64 = maxImageSize + 1024*1024
)

// validation
var (
	errInvalidImageUpload = errors.New("image upload is too large or malformed")
	errMissingImageField  = errors.New("image field is required")
	errEmptyImage         = errors.New("image file is empty")
	errImageTooLarge      = errors.New("image must be no larger than 10 MB")
	errUnsupportedImage   = errors.New("only JPEG and PNG images are supported")
)

// temporarily stores information about the uploaded image before it is saved
type uploadedImage struct {
	data             []byte
	originalFilename string
	contentType      string
	storedFilename   string
}

func (app *application) variantStorageDir() string {
	return filepath.Join(app.config.storageDir, "variants")
}

// creates a secure, random filename for storing images.
func randomFilename(ext string) (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x%s", bytes, ext), nil
}

// createImageHandler accepts one multipart upload, stores the original to disk,
// creates the corresponding image + queued job records, and returns a 202 Accepted.
func (app *application) createImageHandler(w http.ResponseWriter, r *http.Request) {
	// Limit the complete multipart request as well as the image field. This protects
	// the handler when a client sends an inaccurate or missing multipart file size.
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestSize)

	uploaded, err := app.parseUploadedImage(r)
	if err != nil {
		var clientErr error
		switch {
		case errors.Is(err, errInvalidImageUpload),
			errors.Is(err, errMissingImageField),
			errors.Is(err, errEmptyImage),
			errors.Is(err, errImageTooLarge),
			errors.Is(err, errUnsupportedImage):
			clientErr = err
		default:
			app.serverErrorResponse(w, r, err)
			return
		}
		app.badRequestResponse(w, r, clientErr)
		return
	}

	// A file identified as JPEG or PNG is stored and queued even if its bytes
	// are corrupt. The worker performs the decode so the failure becomes a
	// durable job result instead of disappearing during upload acceptance.
	if err := app.storeOriginalImage(uploaded); err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	//This creates the information that will be stored in the images table.
	imageRecord := &data.Image{
		OriginalFilename: filepath.Base(uploaded.originalFilename),
		StoredFilename:   uploaded.storedFilename,
		MediaType:        uploaded.contentType,
		Size:             int64(len(uploaded.data)),
	}

	//creates an empty job object
	job := &data.ImageJob{}

	ctx, cancel := context.WithTimeout(r.Context(), 5e9)
	defer cancel()

	// Both records are created in one transaction. This means a corrupt image
	// still has an image row and a queued job that the worker can later fail.
	if err := app.models.Images.Insert(ctx, imageRecord, job); err != nil {
		_ = os.Remove(filepath.Join(app.config.storageDir, uploaded.storedFilename))
		app.serverErrorResponse(w, r, err)
		return
	}
	app.logJobCommit(data.JobCommit{PublicID: job.PublicID, Version: job.Version, Status: job.Status, Stage: job.Stage, VariantsCompleted: job.VariantsCompleted, SimulationStep: job.SimulationStep}, "accepted")

	//creates the URL where the client can check the job's status.
	statusURL := "/v1/jobs/" + job.PublicID
	headers := make(http.Header)
	headers.Set("Location", statusURL)

	// 202 means acceptance succeeded and processing is still asynchronous.
	// The client follows statusURL to observe queued, processing, or failed.
	app.writeJSON(w, http.StatusAccepted, envelope{
		"image_id":   imageRecord.ID,
		"job_id":     job.PublicID,
		"status":     job.Status,
		"status_url": statusURL,
	}, headers)
}

// parseUploadedImage validates the multipart payload before durable work is created.
// It accepts files identified as JPEG or PNG without decoding them. The
// worker owns decode validation so corrupt uploads remain observable jobs.
func (app *application) parseUploadedImage(r *http.Request) (uploadedImage, error) {
	if err := r.ParseMultipartForm(maxRequestSize); err != nil {
		return uploadedImage{}, errInvalidImageUpload
	}

	file, header, err := r.FormFile("image")
	if err != nil {
		return uploadedImage{}, errMissingImageField
	}
	defer file.Close()

	if header.Size <= 0 {
		return uploadedImage{}, errEmptyImage
	}
	if header.Size > maxImageSize {
		return uploadedImage{}, errImageTooLarge
	}

	dataBytes, err := io.ReadAll(io.LimitReader(file, maxImageSize+1))
	if err != nil {
		return uploadedImage{}, err
	}
	if len(dataBytes) == 0 {
		return uploadedImage{}, errEmptyImage
	}
	if int64(len(dataBytes)) > maxImageSize {
		return uploadedImage{}, errImageTooLarge
	}

	// Byte-sniffing is in charge of choosing the
	// actual type; the header is a weaker fallback, and the extension supplies
	// the type only when neither sniffing nor the header identified it.
	var extContentType string
	switch strings.ToLower(filepath.Ext(header.Filename)) {
	case ".jpg", ".jpeg":
		extContentType = "image/jpeg"
	case ".png":
		extContentType = "image/png"
	default:
		return uploadedImage{}, errUnsupportedImage
	}

	contentType := http.DetectContentType(dataBytes)
	if contentType != "image/jpeg" && contentType != "image/png" {
		contentType = header.Header.Get("Content-Type")
	}
	if contentType != "image/jpeg" && contentType != "image/png" {
		contentType = extContentType
	}

	ext := ".png"
	if contentType == "image/jpeg" {
		ext = ".jpg"
	}

	storedFilename, err := randomFilename(ext)
	if err != nil {
		return uploadedImage{}, err
	}

	return uploadedImage{
		data:             dataBytes,
		originalFilename: header.Filename,
		contentType:      contentType,
		storedFilename:   storedFilename,
	}, nil
}

// storeOriginalImage keeps the original file under a server-controlled filename.
func (app *application) storeOriginalImage(uploaded uploadedImage) error {
	path := filepath.Join(app.config.storageDir, uploaded.storedFilename)
	if err := os.WriteFile(path, uploaded.data, 0600); err != nil {
		return err
	}
	return nil
}

// getVariantHandler serves the requested variant file from disk, or returns a 404 if it is not available.
func (app *application) getVariantHandler(w http.ResponseWriter, r *http.Request) {
	imageID, ok := parseUUIDParam(r.PathValue("id"))
	if !ok {
		app.notFoundResponse(w, r)
		return
	}
	variant, err := app.models.Images.Variant(r.Context(), imageID, r.PathValue("name"))
	if errors.Is(err, data.ErrRecordNotFound) {
		app.notFoundResponse(w, r)
		return
	}
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}
	http.ServeFile(w, r, filepath.Join(app.variantStorageDir(), variant.Filename))
}
