package main

import "net/http"

func (app *application) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/healthcheck", app.healthcheckHandler)
	mux.HandleFunc("GET /v1/jobs/{id}", app.getJobHandler)
	mux.HandleFunc("GET /v1/jobs/{id}/events", app.getJobEventsHandler)
	mux.HandleFunc("POST /v1/images", app.createImageHandler)
	mux.HandleFunc("GET /v1/images/{id}/variants/{name}", app.getVariantHandler)
	mux.Handle("/", noCache(http.FileServer(http.Dir("./frontend"))))
	return mux
}

// noCache makes browsers revalidate static files on every load, so an edited
// frontend module is never served from a stale cache during a measured run.
func noCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		next.ServeHTTP(w, r)
	})
}
