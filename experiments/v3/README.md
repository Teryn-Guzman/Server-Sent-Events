# Assessment 3 — Long polling vs Server-Sent Events

This is the separate Version 3 evidence area. Keep Assessment 2 raw logs and results unchanged. Version 3 uses a fresh browser log store (`imagelab-assessment3-client-log-v1`) and its own server/client captures and generated results.

Write the prediction for each condition in `predictions.md` (this folder) and commit it **before** collecting that condition's runs. Keep the original predictions unchanged and add any later correction as a dated note.

Before collecting the official 18 runs, use a fresh browser profile or clear only the V3 client-log key in DevTools Console with `localStorage.removeItem('imagelab-assessment3-client-log-v1')`. Do not clear the Assessment 2 key or edit downloaded raw logs. Practice/demo runs made with V3 modes and conditions are stored under that separate key; removing it first prevents those from being counted with the assessment runs.

## Workloads and run plan

Collect 18 runs: conditions A, B and C2 × methods long polling and SSE × three repetitions. Use the same image, one foreground browser tab, an empty worker queue before every run, and the same transformation settings. Alternate order within each condition: repetition 1 long then SSE; repetition 2 SSE then long; repetition 3 long then SSE. Select the condition, repetition, and method before each fresh upload. Switching observation modes after acceptance is useful for a demo but should not be used as one of the controlled comparison runs.

| Condition | Server workload | Purpose |
|---|---|---|
| A | `quiet_delay=30s`; no simulated progress | Quiet active period, then real variant processing |
| B | All delays and simulation disabled | Ordinary image processing and real progress |
| C2 | `simulation_duration=10s simulation_interval=200ms` | Labelled simulated updates, then real processing |

For C2, keep the UI condition set to **C2 — Fast simulated updates**. Simulated steps must remain separate from stored variant progress. Do not describe the 200 ms interval as a real transformation update.

## Capture

From the repository root, create a new evidence directory and append server JSON logs for every server restart:

```sh
mkdir -p experiments/v3/raw
make run/api quiet_delay=30s 2>&1 | tee -a experiments/v3/raw/server.jsonl
```

Stop the server with Ctrl+C between conditions, then restart using the condition’s flags:

```sh
make run/api 2>&1 | tee -a experiments/v3/raw/server.jsonl
make run/api simulation_duration=10s simulation_interval=200ms 2>&1 | tee -a experiments/v3/raw/server.jsonl
```

The `run/api` recipe must start with `@go run …` so Make does not echo the expanded command, which contains the database password, into the log. After the first capture, check that nothing leaked and that the log is clean JSON:

```sh
grep -c "postgres://" experiments/v3/raw/server.jsonl   # must print 0
```

On Ctrl+C Make may print a `make: *** … Error 130` line. It is not JSON, so the analyzer skips it and counts it under "non-JSON lines skipped"; that is expected.

Run the full three-repetition, two-method set for each condition. Keep DevTools open, preserve the Network log, and leave the browser tab in the foreground. In the app, select **Long polling** or **Server-Sent Events (SSE)** before uploading. The SSE mode observes the accepted job at `/v1/jobs/{id}/events`; it does not upload again. For a reconnect check, briefly toggle DevTools Network Offline then Online and confirm the same EventSource reconnects and receives a current snapshot. Do not claim that SSE replays every missed version; this implementation sends current snapshots and does not provide `id` or `Last-Event-ID` replay.

After the runs, use **Download raw client logs** and save the downloaded file as `experiments/v3/raw/client.jsonl`. Keep raw files unchanged. The server JSON records include committed job versions and post-commit time, stream openings/closures, event and heartbeat counts, observer database reads, stream duration, and peak active SSE handlers. The client records include poll responses, SSE stream opens/errors, received snapshot versions, render receipt times, and terminal state.

## Analyze

Run the separate Version 3 analyzer (Python 3.11 or newer; `py` on Windows):

```sh
python3 experiments/v3/analyze.py \
  --client experiments/v3/raw/client.jsonl \
  --server experiments/v3/raw/server.jsonl \
  --out-dir experiments/v3/results
```

To leave one run out (for example an interrupted attempt that was repeated), add `--exclude-job JOB_ID_PREFIX` (at least 8 characters of its job ID, repeatable). It must match exactly one run, the raw logs are never modified, and excluded runs are listed in `summary.md`.

It writes a run-level CSV, Markdown run and median/range tables, and two SVG graphs. The request comparison counts long-poll observation GETs against SSE stream GETs (one `sse stream opened` server record per stream, cross-checked against the client's `sse_stream_open` events), not TCP connections. Heartbeats are comments, not job-update events. Each graph shows all three individual repetitions, the median and the min–max whisker. Review the run-count check and every non-empty note before drawing conclusions.

## Interpretation limits

The SSE endpoint sends a complete PostgreSQL-backed snapshot immediately on connection and after a newer committed version is observed. In-process notifications are wake-up signals and can coalesce. A disconnect ends that stream handler but not the image job; EventSource reconnects and receives the latest snapshot. Intermediate versions may be skipped. Request/stream counts alone do not measure total resource cost, and these single-observer classroom results do not establish production-scale capacity.

The server sends an optional heartbeat comment every 15 seconds. It is not a job event; besides helping idle streams through intermediaries, it resets the response write deadline so the existing 25-second HTTP write timeout does not cut off the 30-second quiet workload.