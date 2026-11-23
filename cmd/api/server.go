package main

import (
	"errors"
	"fmt"
	"net/http"
	"time"
)

func (app *application) serve() error {
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", app.config.port),
		Handler:      app.recoverPanic(app.logRequest(app.cors(app.routes()))),
		IdleTimeout:  time.Minute,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 25 * time.Second,
	}

	// Open SSE streams never finish on their own, so tell them to stop when
	// Shutdown begins; otherwise Shutdown waits for its full timeout.
	srv.RegisterOnShutdown(app.beginShutdown)

	shutdownError := app.gracefulShutdown(srv)

	app.logger.Info("starting server", "addr", srv.Addr, "env", app.config.env)

	err := srv.ListenAndServe()
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	err = <-shutdownError
	if err != nil {
		return err
	}

	app.logger.Info("stopped server", "addr", srv.Addr)

	return nil
}
