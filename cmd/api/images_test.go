package main

import (
	"bytes"
	"image"
	"image/color"
	"mime/multipart"
	"net/http/httptest"
	"net/textproto"
	"testing"
	"time"
)

func TestResizeVariantContracts(t *testing.T) {
	source := image.NewRGBA(image.Rect(0, 0, 100, 50))
	source.Set(50, 25, color.RGBA{R: 255, A: 255})

	tests := []struct {
		name          string
		width, height int
		crop          bool
		wantWidth     int
		wantHeight    int
	}{
		{name: "thumbnail crop", width: 150, height: 150, crop: true, wantWidth: 150, wantHeight: 150},
		{name: "preview keeps aspect ratio", width: 800, height: 600, wantWidth: 800, wantHeight: 400},
		{name: "display keeps aspect ratio", width: 1200, height: 900, wantWidth: 1200, wantHeight: 600},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output, width, height := resize(source, test.width, test.height, test.crop)
			if width != test.wantWidth || height != test.wantHeight {
				t.Fatalf("resize dimensions = %dx%d, want %dx%d", width, height, test.wantWidth, test.wantHeight)
			}
			if bounds := output.Bounds(); bounds.Dx() != test.wantWidth || bounds.Dy() != test.wantHeight {
				t.Fatalf("output bounds = %v, want %dx%d", bounds, test.wantWidth, test.wantHeight)
			}
		})
	}
}

func TestParseUploadedImageAcceptsCorruptPNGForWorker(t *testing.T) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("image", "corrupt.png")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write([]byte("not a valid PNG")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest("POST", "/v1/images", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	uploaded, err := (&application{}).parseUploadedImage(request)
	if err != nil {
		t.Fatalf("parseUploadedImage() rejected corrupt PNG before worker: %v", err)
	}
	if uploaded.contentType != "image/png" {
		t.Fatalf("content type = %q, want image/png", uploaded.contentType)
	}
	if len(uploaded.data) == 0 {
		t.Fatal("corrupt image bytes were not retained for worker processing")
	}
}

func TestParseUploadedImageRejectsUnsupportedExtensionDespitePNGContentType(t *testing.T) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", `form-data; name="image"; filename="animated.gif"`)
	header.Set("Content-Type", "image/png")
	part, err := writer.CreatePart(header)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write([]byte("not a PNG")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest("POST", "/v1/images", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	if _, err := (&application{}).parseUploadedImage(request); err != errUnsupportedImage {
		t.Fatalf("parseUploadedImage() error = %v, want %v", err, errUnsupportedImage)
	}
}

func TestParseJobRuntimeConfigAcceptsManualDelayOverrides(t *testing.T) {
	app := &application{config: config{imageProcessingDelay: 250 * time.Millisecond, quietDelay: 500 * time.Millisecond, simulationDuration: 2 * time.Second, simulationInterval: 200 * time.Millisecond}}
	request := httptest.NewRequest("POST", "/v1/images", bytes.NewBufferString("delay_ms=900ms&quiet_delay_ms=1s&simulation_duration_ms=3s&simulation_interval_ms=150ms"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err := request.ParseForm(); err != nil {
		t.Fatal(err)
	}

	cfg, err := app.parseJobRuntimeConfig(request)
	if err != nil {
		t.Fatalf("parseJobRuntimeConfig() unexpected error: %v", err)
	}
	if cfg.imageProcessingDelay != 900*time.Millisecond {
		t.Fatalf("delay = %v, want 900ms", cfg.imageProcessingDelay)
	}
	if cfg.quietDelay != time.Second {
		t.Fatalf("quiet_delay = %v, want 1s", cfg.quietDelay)
	}
	if cfg.simulationDuration != 3*time.Second {
		t.Fatalf("simulation_duration = %v, want 3s", cfg.simulationDuration)
	}
	if cfg.simulationInterval != 150*time.Millisecond {
		t.Fatalf("simulation_interval = %v, want 150ms", cfg.simulationInterval)
	}
}

func TestParseJobRuntimeConfigRejectsInvalidSimulationValues(t *testing.T) {
	app := &application{config: config{}}
	request := httptest.NewRequest("POST", "/v1/images", bytes.NewBufferString("simulation_duration_ms=3s&simulation_interval_ms=0"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err := request.ParseForm(); err != nil {
		t.Fatal(err)
	}

	if _, err := app.parseJobRuntimeConfig(request); err != errInvalidDelayConfig {
		t.Fatalf("parseJobRuntimeConfig() error = %v, want %v", err, errInvalidDelayConfig)
	}
}
