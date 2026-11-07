package main

import (
	"context"
	"database/sql"
	"flag"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
	"github.com/teryn-guzman/gatekeeper-asynchronous/internal/data"
)

type config struct {
	port                 int
	env                  string
	workerPollInterval   time.Duration
	imageProcessingDelay time.Duration
	quietDelay           time.Duration
	simulationDuration   time.Duration
	simulationInterval   time.Duration
	storageDir           string
	db                   struct {
		dsn          string
		maxOpenConns int
		maxIdleConns int
		maxIdleTime  time.Duration
	}
}

type application struct {
	config       config
	logger       *slog.Logger
	models       data.Models
	jobNotifier  *jobNotifier
	sseJobReader jobSnapshotReader
	wg           sync.WaitGroup
	workerCancel context.CancelFunc
	sseStreamSeq atomic.Uint64
	sseMetricsMu sync.Mutex
	activeSSE    atomic.Int64
	peakSSE      atomic.Int64
}

func main() {
	var cfg config

	flag.IntVar(&cfg.port, "port", 4000, "API server port")
	flag.StringVar(&cfg.env, "env", "development", "Environment (development|staging|production)")
	flag.DurationVar(&cfg.workerPollInterval, "worker-poll-interval", 250*time.Millisecond, "Worker queue-check interval")
	flag.DurationVar(&cfg.imageProcessingDelay, "image-processing-delay", 0, "Artificial delay inside the image worker")
	flag.DurationVar(&cfg.quietDelay, "quiet-delay", 0, "Experimental processing delay with no intermediate progress updates")
	flag.DurationVar(&cfg.simulationDuration, "simulation-duration", 0, "Experimental simulated-progress phase duration (disabled at 0)")
	flag.DurationVar(&cfg.simulationInterval, "simulation-interval", 0, "Experimental simulated-progress update interval (disabled at 0)")
	flag.StringVar(&cfg.storageDir, "storage-dir", "./storage", "Directory for original and generated images")

	flag.StringVar(&cfg.db.dsn, "db-dsn", "", "PostgreSQL DSN")

	flag.IntVar(&cfg.db.maxOpenConns, "db-max-open-conns", 25, "PostgreSQL max open connections")
	flag.IntVar(&cfg.db.maxIdleConns, "db-max-idle-conns", 25, "PostgreSQL max idle connections")
	flag.DurationVar(&cfg.db.maxIdleTime, "db-max-idle-time", 15*time.Minute, "PostgreSQL max connection idle time")

	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if cfg.quietDelay < 0 || cfg.simulationDuration < 0 || cfg.simulationInterval < 0 || (cfg.simulationDuration > 0 && cfg.simulationInterval == 0) {
		logger.Error("invalid experimental workload configuration; durations cannot be negative and simulation requires an interval")
		os.Exit(1)
	}

	db, err := openDB(cfg)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	defer db.Close()

	logger.Info("database connection pool established")
	logger.Info("experiment workload configuration",
		"image_processing_delay", cfg.imageProcessingDelay.String(),
		"quiet_delay", cfg.quietDelay.String(),
		"simulation_duration", cfg.simulationDuration.String(),
		"simulation_interval", cfg.simulationInterval.String(),
	)

	models := data.NewModels(db)
	app := &application{
		config:       cfg,
		logger:       logger,
		models:       models,
		sseJobReader: models.Images,
		jobNotifier:  newJobNotifier(),
	}
	if err := os.MkdirAll(cfg.storageDir, 0755); err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	if err := os.MkdirAll(app.variantStorageDir(), 0755); err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	// Q44: When the app receives SIGINT, it starts shutting down instead of
	// immediately stopping. The worker is separate from the HTTP server, so it
	// can still be running while the HTTP server begins to shut down.

	// Q45: srv.Shutdown stops the server from accepting new connections and gives
	// the HTTP requests that are already running time to finish.

	// Q46: I need to cancel the worker before calling app.wg.Wait() because otherwise
	// the worker could keep running and Wait would have nothing telling it to stop.

	// Q47: The defer in main is not enough because defer only runs when main is
	// ending. I need to cancel the worker as part of the actual shutdown process.

	// Q48: ctx.Done tells the worker that it should stop, app.wg.Done tells the
	// wait group that the worker has finished, and app.wg.Wait makes the program
	// wait until that actually happens before shutting down completely.
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	app.workerCancel = cancelWorker
	defer cancelWorker()
	app.startImageWorker(workerCtx)

	err = app.serve()
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}

func openDB(cfg config) (*sql.DB, error) {
	db, err := sql.Open("postgres", cfg.db.dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(cfg.db.maxOpenConns)
	db.SetMaxIdleConns(cfg.db.maxIdleConns)
	db.SetConnMaxIdleTime(cfg.db.maxIdleTime)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}
