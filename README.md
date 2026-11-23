# ImageLab

## Teryn Guzman & Kelvin Gordon

ImageLab is an asynchronous image-processing application. The browser uploads
one JPEG or PNG, the Go API durably records the original and a queued job, and
one in-process worker generates image variants independently of the upload
request.

The API returns `202 Accepted` when the original image and queued job have been
stored. The browser can observe the job with one-second short polling,
version-based long polling, or a Server-Sent Events (SSE) stream until the job
is `completed` or `failed`.

## Requirements

- Go version declared in `go.mod`
- PostgreSQL
- `psql`
- The `migrate` CLI

The migrations use PostgreSQL extensions/functions supplied by the starter
project, including `citext`, `uuidv7()`, and `uuidv4()`.

## Setup

Create a local environment file and edit the connection details if needed:

```bash
cp .envrc.example .envrc
source .envrc
```

The DSN points to the local `image_lab` database using the `image_lab` role:

```text
postgres://image_lab:password@localhost:5432/image_lab?sslmode=disable
```

The database and role must exist, and `citext` must be enabled. Apply the
project migrations to create/update the application tables (including the
versioned-progress migration):

```bash
make db/migrations/up
```

The migration command asks for confirmation. `.envrc` is ignored by Git and
should not be committed.

## Run the application

Start the API and keep this terminal open:

```bash
source .envrc
make run/api
```

For a visible polling demonstration, use a worker-only delay. Upload acceptance
still returns immediately; the job remains in `processing` during the delay:

```bash
make run/api image_processing_delay=10s
```

Open the browser at:

```text
http://localhost:4000/
```

The API serves the frontend from `frontend/`, so do not open
`frontend/index.html` directly from the filesystem.

## Image workflow

1. Select a JPEG or PNG no larger than 10 MB.
2. The browser shows a local preview without sending a request.
3. Click **Process image** once.
4. `POST /v1/images` validates and stores the original image, creates a queued
   PostgreSQL job, and returns `202 Accepted`.
5. The browser observes the job using the selected mode: short polling, long
   polling, or SSE (see Assessment 3 below).
6. The worker claims the queued job, creates all three variants, records their
   metadata, and marks the job `completed`.
7. The browser stops polling and displays the generated images.

The allowed job states are:

```text
queued -> processing -> completed
                       or failed
```

Retrieval errors are treated as observation failures. The browser preserves the
last known job and offers **Try again** instead of falsely marking the job as
failed or uploading the image again. If the initial `POST /v1/images` itself
fails (for example, a lost connection before any job exists), the UI instead
offers **Retry upload**, which resubmits the same selected file.

## Assessment 2: versioned observation

The same PostgreSQL job is authoritative for both modes. Jobs start at version
1; a version increases atomically with committed observable status/progress
changes. `GET /v1/jobs/{id}` remains a prompt snapshot. A long poll uses
`GET /v1/jobs/{id}?after=VERSION` and waits up to 20 seconds for a newer version.
If the active job is unchanged at the holding deadline, the API returns an
empty `204`; the browser immediately starts another long poll. A notification
only wakes observers to re-read PostgreSQL; it is not a substitute for durable
state. The server registers each observer before its initial read, broadcasts
to all local subscribers after commit, and coalesces duplicate wakeups. The
handler rechecks the database after every wake and before `204`, preventing a
commit between the initial check and wait from being overlooked.
Because queued job representations include `queue_position`, claiming a job
also increments the versions of remaining queued jobs whose positions changed
in the same transaction and notifies their observers after commit.

Each real variant milestone stores its file metadata and increments the
`variants_completed`, `stage`, and `version` fields in one transaction. The
third milestone also marks the job completed in that transaction. Experimental
simulated progress is separately recorded in `simulation_step`; it never
changes the real variant count or causes completion. Client cancellation stops
observation only; the worker continues.

Select **Short polling** or **Long polling** before processing an image. Mode
switching cancels the old request and resumes observation of the same job; it
does not POST another image. The request timeout is 30 seconds. A normal 204 is
not a retrieval error. For terminal jobs, observation stops. On a retrieval
failure the UI preserves the last representation and **Try again** observes the
same job.

The migration `000011_add_versioned_job_progress` backfills existing jobs with
version 1 and derives stored-variant progress. Apply it with
`make db/migrations/up`. Server experiment options default to disabled:

| Make variables | Example | Purpose |
| --- | --- | --- |
| `quiet_delay` | `quiet_delay=30s` | Delay work without intermediate changes (Condition A) |
| `simulation_duration` | `simulation_duration=10s` | Experimental simulated phase duration |
| `simulation_interval` | `simulation_interval=2s` or `200ms` | Simulated update interval (C1/C2/C3) |

Examples and reproducible procedure are in [experiments/README.md](experiments/README.md).
The page includes run condition/repetition/latency metadata and **Download raw
client logs**. Server output is JSON Lines; log its stdout as documented, then
join the raw client and server files with `experiments/analyze.py`. That script
generates the required CSV, per-run/grouped comparison tables, and labelled
SVG charts from actual log records. Predictions, raw logs and results are in
`experiments/` (`predictions.md`, `raw/`, `results/`); see
[experiments/README.md](experiments/README.md) for the procedure, the exact
analyzer command and the run notes.

**Baseline:** Assessment 2 work started from commit
`e03682d055cf31a32cfca75103fde4b26a959c52` (`intial setup`), the first commit of
this repository, tagged `assessment1-baseline`. It contains the Assessment 1
application that Assessment 2 extends.

## Assessment 3: Server-Sent Events

SSE is a third way to observe a job. Short polling and long polling still work.
The upload, durable job, single worker, three variants and all-or-complete
results are unchanged; only the way the browser learns about job changes is new.

### Mode selection

Choose **Server-Sent Events (SSE)** in the *Observation mode* dropdown before
clicking **Process image**. Switching modes after a job is accepted observes the
same job and never uploads again. **Stop observing** closes the stream and the
page shows **Resume observation**, which opens one new stream for the same job.
Stopping observation never stops the worker.

### Endpoint

```http
GET /v1/jobs/{id}/events
Accept: text/event-stream
```

- Unknown or malformed job IDs return `404` before any stream begins.
- Responses use `Content-Type: text/event-stream; charset=utf-8`, `Cache-Control:
  no-cache` and `X-Accel-Buffering: no`.
- On connection the server immediately sends the current complete job snapshot,
  then another complete snapshot each time it observes a newer committed version.
  A snapshot is not a delta; a slow observer may skip intermediate versions.
- Each frame is flushed and ends with a blank line:

```text
event: job-update
data: {"id":"...","status":"processing","version":3,"stage":"thumbnail_ready","variants_completed":1,...}

```

- After the terminal (`completed` or `failed`) snapshot the server ends the stream
  and the browser calls `EventSource.close()`.
- An idle stream receives a `: heartbeat` comment every 15 seconds. A heartbeat is
  not a job event and does not change the job version. It also resets the response
  write deadline so the 25 s HTTP write timeout does not cut off a quiet job.
- No `id:` fields are sent and there is no `Last-Event-ID` replay. A reconnecting
  `EventSource` makes a new GET and receives the latest snapshot.

Try it from a terminal (`-N` turns off curl's buffering so frames appear as they
are flushed):

```bash
curl -N http://localhost:4000/v1/jobs/JOB_ID/events
```

### How a change reaches every tab

The worker commits a new version to PostgreSQL, then publishes an in-process
wake-up to every stream subscribed to that job. A wake-up only means "read again":
each handler re-reads the job from PostgreSQL and sends it only if its version is
newer than the last one sent on that stream. Each handler subscribes before its
first read, so a commit cannot fall between the read and the wait.

### Disconnects and shutdown

- If the connection drops, the browser keeps the same `EventSource`, shows
  *Reconnecting to job stream*, and receives the latest snapshot when it
  reconnects. The job is never marked failed because of a connection problem.
- If the browser gives up (the stream reaches `CLOSED`, for example after a
  `404`/`500` on reconnect), the page shows **Unable to check status** with **Try
  again**, which opens one new stream for the same job.
- Closing a stream, or shutting the server down with Ctrl+C, ends only the
  observation handlers; the worker and the job are unaffected. On shutdown every
  open stream returns immediately instead of holding the server open.

### Experiment

The SSE vs long-polling comparison (conditions A, B and C2, three repetitions
each, 18 runs) is documented in [experiments/v3/README.md](experiments/v3/README.md),
including server flags, log capture and the analyzer command. Assessment 2 files
under `experiments/` are kept unchanged.

## HTTP API

### Upload an image

```http
POST /v1/images
Content-Type: multipart/form-data
```

The multipart field is named `image`. A successful response is `202 Accepted`
with a `Location` header and a body like:

```json
{
  "image_id": "...",
  "job_id": "...",
  "status": "queued",
  "status_url": "/v1/jobs/..."
}
```

Example:

```bash
curl --include --form image=@/path/to/photo.jpg \
  http://localhost:4000/v1/images
```

### Check a job

```bash
curl http://localhost:4000/v1/jobs/JOB_ID
```

A completed job includes metadata for `thumbnail`, `preview`, and `display`.
Variant metadata is returned only once the job is completed; before that,
progress is reported through `variants_completed` and `stage`. A malformed or
unknown job ID returns 404.

### Observe a job with SSE

```text
GET /v1/jobs/{id}/events
```

Streams `job-update` events as described in
[Assessment 3: Server-Sent Events](#assessment-3-server-sent-events). A
malformed or unknown ID returns 404.

### Retrieve a variant

```text
GET /v1/images/{image_id}/variants/{name}
```

Variants of completed jobs are served from the local storage directory; before
the job completes the endpoint returns 404. Arbitrary file paths are not
exposed.

## Variants

| Variant | Contract |
| --- | --- |
| `thumbnail` | Exact 150 x 150 square crop |
| `preview` | Fits within 800 x 600 while preserving aspect ratio |
| `display` | Fits within 1200 x 900 while preserving aspect ratio |

The database stores image, job, and variant metadata. Originals are stored in
`storage/`; generated files are stored in `storage/variants/`. Both directories
are created when the API starts.

## Database inspection

```bash
make db/psql
```

Useful queries:

```sql
SELECT * FROM images ORDER BY created_at DESC;
SELECT * FROM image_jobs ORDER BY queued_at DESC;
SELECT * FROM image_variants ORDER BY created_at DESC;
```

## Week 3/4 acceptance demonstration

Run these steps in order. Steps 3 onward use a second terminal, with the
server from step 3 left running throughout.

### 1. Automated tests

```bash
go test ./... -v
```

### 2. Sample files

`tmp/` should not be committed.

```bash
mkdir -p tmp
echo "tmp/" >> .gitignore
echo 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==' | base64 -d > tmp/tiny.png
printf 'not a valid PNG' > tmp/corrupt.png
echo hello > tmp/note.txt
```

### 3. Start the server

```bash
make run/api image_processing_delay=10s
```

The delay makes the async behavior in the steps below observable.

### 4. Required outcomes and failures

- Reject an empty or unsupported file and confirm no job is queued:

  ```bash
  curl -s -o /dev/null -w "text file: %{http_code}\n" \
    -F "image=@tmp/note.txt;type=text/plain" http://localhost:4000/v1/images
  curl -s -o /dev/null -w "no file:   %{http_code}\n" \
    -X POST http://localhost:4000/v1/images
  ```

- Submit a corrupt `.png` payload with a PNG multipart content type. POST should
  return `202`; the worker should later produce `status = failed`, a safe
  `error_message`, and `failed_at`.

  ```bash
  JOB=$(curl -s -F "image=@tmp/corrupt.png;type=image/png" \
    http://localhost:4000/v1/images | grep -o '"job_id": *"[^"]*"' | cut -d'"' -f4)
  sleep 12; curl -s http://localhost:4000/v1/jobs/$JOB
  ```

- Upload a valid image and confirm the original is in `storage/` and the three
  output files are in `storage/variants/`:

  ```bash
  curl -i -w "\nack: %{time_total}s\n" \
    -F "image=@tmp/tiny.png;type=image/png" http://localhost:4000/v1/images
  ls storage storage/variants
  ```

- After a terminal state, use **Choose another image** and submit a second image.

### 5. Five-image burst (queue position with one worker)

```bash
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w "ack $i: %{http_code} %{time_total}s\n" \
    -F "image=@tmp/tiny.png;type=image/png" http://localhost:4000/v1/images
done

# Wait until all five have reached a terminal state before reading the table.
while [ "$(psql "$GATEKEEPER_DB_DSN" -tAc \
  "SELECT COUNT(*) FROM image_jobs WHERE status IN ('queued','processing') \
   AND queued_at > now() - interval '2 minutes';")" != "0" ]; do
  sleep 2
done

psql "$GATEKEEPER_DB_DSN" -c "SELECT public_id, status, \
  started_at - queued_at AS queue_wait, \
  COALESCE(completed_at, failed_at) - started_at AS processing, \
  COALESCE(completed_at, failed_at) - queued_at AS total \
  FROM image_jobs ORDER BY queued_at DESC LIMIT 5;"
```

`queue_wait` should climb by roughly one delay interval (~10s) per job, since
one worker processes them strictly in order. For the single-image measurement
row, run one upload by itself and read the same query with `LIMIT 1`. To see
live `queue_position` values, poll a queued job's `status_url` during the
burst instead of waiting for it to finish.

### 6. Polling and retry (browser)

1. Open `http://localhost:4000`. In DevTools **Network**, enable **Preserve
   log** and filter for `/v1/jobs/`.
2. Upload and process an image. Confirm the POST returns `202` before the
   delay ends, then observe separate status GET requests about one second
   apart.
3. Confirm the card moves through `queued`/`processing` to `completed`,
   polling stops, and all three variants appear.
4. To simulate an observation error, switch Network throttling to **Offline**
   after the job is accepted. The UI should show **Unable to check status**,
   keep the last known job state, and offer **Try again**. Go back online and
   retry; it must observe the same job URL without a new POST.
5. To simulate a submission failure, stop the server (or go Offline) before
   clicking **Process image**. The UI should show **Upload failed** with a
   **Retry upload** button. Restart the server (or go back online) and click
   it; it must resubmit the same selected file without reselecting it.

### 7. SSE checks (browser)

1. Start the server with `make run/api image_processing_delay=10s`, open
   `http://localhost:4000`, and choose **Server-Sent Events (SSE)**.
2. Process an image. In DevTools **Network**, select the `/events` request and
   open its **EventStream** tab: one request stays pending and `job-update`
   frames arrive as the job changes, with no repeated GETs.
3. Watch the same job from a second observer. The page only observes jobs it
   uploaded itself, so open a second browser tab, open its DevTools **Console**
   and run (with the real job ID):

   ```js
   const es = new EventSource('/v1/jobs/JOB_ID/events');
   es.addEventListener('job-update', e => { const j = JSON.parse(e.data); console.log(j.version, j.status, j.stage); });
   ```

   or run `curl -N http://localhost:4000/v1/jobs/JOB_ID/events` in a terminal.
   Both observers receive every update for the job.
4. Toggle DevTools Network **Offline** then **Online** while the job runs. The
   page shows **Reconnecting to job stream**, the same stream reconnects, the
   latest snapshot arrives, and the job is not marked failed.
5. Click **Stop observing**. The stream closes, **Observation stopped** appears,
   and the job still reaches `completed` on the server (check with
   `curl http://localhost:4000/v1/jobs/JOB_ID`). **Resume observation** opens a
   new stream for the same job without another POST.
6. Let a job finish while observing: the final snapshot renders once, the
   stream closes, and no reconnect request follows.

## Validation and troubleshooting

Run the Go tests and frontend syntax check:

```bash
go test ./...
node --check frontend/app.js
```

Check that the API is running:

```bash
curl http://localhost:4000/v1/healthcheck
```

If the browser page is blank or appears stale, open `http://localhost:4000/`
and hard-refresh with `Ctrl+Shift+R`. Do not open the HTML file directly.

If an upload does not appear in the Network panel, the request was stopped in
the browser before it reached the API. Check that the selected file is a JPEG
or PNG under 10 MB and that **Process image** is enabled.

The application intentionally uses one worker, PostgreSQL, local filesystem
storage, HTTP polling, long polling, Server-Sent Events, and vanilla JavaScript.
It has no WebSockets, message broker, or multi-server fan-out.