#!/usr/bin/env python3
"""Summarize ImageLab Assessment 3 long-polling and SSE JSONL evidence."""

import argparse
import csv
import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

if sys.version_info < (3, 11):
    raise SystemExit(
        "analyze.py needs Python 3.11 or newer: Go logs nanosecond RFC 3339 timestamps "
        "that older versions of datetime.fromisoformat cannot parse."
    )

CONDITIONS = ["A", "B", "C2"]
METHODS = ["long", "sse"]
CSV_COLUMNS = [
    "run_id", "condition", "mode", "repetition", "job_id", "image",
    "latency_setting", "baseline_version", "terminal_version",
    "observation_gets", "sse_stream_gets", "job_events_received", "heartbeats",
    "versions_received", "version_gaps", "processing_ms", "detection_ms",
    "observation_db_reads", "stream_seconds", "peak_active_streams", "notes",
]
EXPECTED_WORKLOAD = {
    "A": {"quiet_delay": "30s", "simulation_duration": "0s", "simulation_interval": "0s", "image_processing_delay": "0s"},
    "B": {"quiet_delay": "0s", "simulation_duration": "0s", "simulation_interval": "0s", "image_processing_delay": "0s"},
    "C2": {"quiet_delay": "0s", "simulation_duration": "10s", "simulation_interval": "200ms", "image_processing_delay": "0s"},
}


def read_jsonl(path):
    records, skipped = [], []
    for number, line in enumerate(Path(path).read_text(encoding="utf-8-sig", errors="replace").splitlines(), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
            if isinstance(record, dict):
                records.append(record)
            else:
                skipped.append(number)
        except json.JSONDecodeError:
            skipped.append(number)
    return records, skipped


def epoch_ms(value):
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000


def median_range(values):
    values = [float(value) for value in values if value is not None and value != ""]
    if not values:
        return "n/a"
    return f"{statistics.median(values):g} ({min(values):g}–{max(values):g})"


def find_excluded_runs(client_runs, prefixes):
    """Map each --exclude-job prefix to exactly one client run_id."""
    excluded = {}
    for prefix in prefixes:
        if len(prefix) < 8:
            raise SystemExit(f"--exclude-job {prefix!r}: use at least 8 characters of the job ID.")
        matches = sorted({
            run_id for run_id, events in client_runs.items()
            if any(str(event.get("job_id", "")).startswith(prefix) for event in events)
        })
        if len(matches) != 1:
            raise SystemExit(f"--exclude-job {prefix!r} must match exactly one run; matched {len(matches)}.")
        excluded[matches[0]] = prefix
    return excluded


def summarize(client_records, server_records, exclude_prefixes=()):
    client_runs = defaultdict(list)
    for record in client_records:
        if record.get("run_id"):
            client_runs[record["run_id"]].append(record)
    excluded_ids = find_excluded_runs(client_runs, exclude_prefixes)
    excluded_runs = []
    for run_id in excluded_ids:
        events = client_runs.pop(run_id)
        first = next((event for event in events if event.get("job_id")), events[0])
        excluded_runs.append({
            "job_id": first.get("job_id", ""), "condition": first.get("condition", ""),
            "mode": first.get("mode", ""), "repetition": first.get("repetition", ""),
            "events": len(events),
        })
    server_by_job = defaultdict(list)
    for record in server_records:
        if record.get("job_id"):
            server_by_job[record["job_id"]].append(record)

    output = []
    ignored_runs = 0
    for run_id, events in sorted(client_runs.items()):
        events.sort(key=lambda event: event.get("client_time_ms", 0))
        metadata = next((event for event in events if event.get("job_id")), events[0])
        job_id = metadata.get("job_id", "")
        mode = metadata.get("mode", "")
        condition = metadata.get("condition", "")
        if condition not in CONDITIONS or mode not in METHODS:
            ignored_runs += 1
            continue
        server_events = server_by_job.get(job_id, [])

        snapshot_events = [
            event for event in events
            if event.get("event") in ("response_received", "sse_job_update_received")
            and event.get("received_version") is not None
        ]
        received = [int(event["received_version"]) for event in snapshot_events]
        terminal_events = [event for event in snapshot_events if event.get("job_status") in ("completed", "failed")]
        terminal_event = terminal_events[-1] if terminal_events else None
        baseline = received[0] if received else ""
        terminal_version = terminal_event.get("received_version", "") if terminal_event else ""
        terminal_status = terminal_event.get("job_status", "") if terminal_event else ""

        committed = [
            event for event in server_events
            if event.get("msg") == "job committed" and isinstance(event.get("version"), int)
        ]
        committed_versions = {int(event["version"]) for event in committed}
        gaps = []
        if baseline != "" and terminal_version != "":
            gaps = sorted(
                version for version in committed_versions
                if baseline < version <= int(terminal_version) and version not in set(received)
            )

        detection_ms = ""
        processing_ms = ""
        if terminal_event:
            terminal_commit = next((event for event in reversed(committed)
                                    if event.get("version") == int(terminal_version)
                                    and event.get("status") == terminal_status
                                    and event.get("post_commit_at")), None)
            if terminal_commit:
                detection_ms = round(epoch_ms(terminal_event.get("client_time_ms")) - epoch_ms(terminal_commit["post_commit_at"]))
            terminal_at = terminal_event.get("completed_at") or terminal_event.get("failed_at")
            if terminal_event.get("started_at") and terminal_at:
                processing_ms = round(epoch_ms(terminal_at) - epoch_ms(terminal_event["started_at"]))

        stream_open_records = [event for event in server_events if event.get("msg") == "sse stream opened"]
        stream_close_records = [event for event in server_events if event.get("msg") == "sse stream closed"]
        heartbeat_records = [event for event in server_events if event.get("msg") == "sse heartbeat"]
        sent_sse_events = sum(event.get("msg") == "sse job-update sent" for event in server_events)
        # The server's 'request started' log has no job_id, so it can never be joined to a
        # run. Each opened stream logs 'sse stream opened' with its job_id: count those.
        stream_gets = len(stream_open_records)
        client_stream_opens = sum(event.get("event") == "sse_stream_open" for event in events)
        reads = sum(int(event.get("queries", 0)) for event in server_events if event.get("msg") == "observation database reads")
        reads += sum(int(event.get("database_reads", 0)) for event in stream_close_records)
        stream_seconds = sum(float(event.get("duration_ms", 0)) for event in stream_close_records) / 1000
        peak_streams = max(
            (int(event.get("peak_active_streams", 0)) for event in stream_open_records + stream_close_records),
            default=0,
        )

        notes = []
        if not terminal_event:
            notes.append("terminal snapshot not received")
        if terminal_event and detection_ms == "":
            notes.append("terminal commit not joined")
        if detection_ms != "" and detection_ms < 0:
            notes.append("negative detection delay; inspect clock ordering")
        poll_requests = [event for event in events if event.get("event") == "request_start"]
        completed_ids = {
            event.get("request_id") for event in events
            if event.get("event") in ("response_received", "retrieval_error", "request_cancelled")
        }
        if any(event.get("request_id") not in completed_ids for event in poll_requests):
            notes.append("poll request still pending at log end")
        if mode == "sse" and stream_gets == 0:
            notes.append("no server 'sse stream opened' record found for this job")
        if mode == "sse" and stream_gets != client_stream_opens:
            notes.append(f"server opened {stream_gets} stream(s), client logged {client_stream_opens}")
        if mode == "sse" and len(stream_close_records) < len(stream_open_records):
            notes.append("stream close record missing")
        received_sse_events = sum(event.get("event") == "sse_job_update_received" for event in events)
        if mode == "sse" and sent_sse_events != received_sse_events:
            notes.append(f"server sent {sent_sse_events} job event(s), client received {received_sse_events}")

        workload_records = [event for event in server_records if event.get("msg") == "experiment workload configuration"]
        job_times = [epoch_ms(event.get("time")) for event in server_events if event.get("time")]
        first_job_time = min(job_times) if job_times else None
        eligible = [event for event in workload_records if first_job_time is not None and epoch_ms(event.get("time")) <= first_job_time]
        workload = max(eligible, key=lambda event: epoch_ms(event.get("time"))) if eligible else None
        if not workload:
            notes.append("server workload configuration not found")
        else:
            for key, expected in EXPECTED_WORKLOAD[condition].items():
                if str(workload.get(key, "")) != expected:
                    notes.append(f"workload mismatch: {key}={workload.get(key)} expected {expected}")

        output.append({
            "run_id": run_id,
            "condition": condition,
            "mode": mode,
            "repetition": metadata.get("repetition", ""),
            "job_id": job_id,
            "image": metadata.get("image", ""),
            "latency_setting": metadata.get("latency_setting", ""),
            "baseline_version": baseline,
            "terminal_version": terminal_version,
            "observation_gets": sum(event.get("event") == "request_start" for event in events) if mode == "long" else 0,
            "sse_stream_gets": stream_gets if mode == "sse" else 0,
            "job_events_received": received_sse_events if mode == "sse" else 0,
            "heartbeats": len(heartbeat_records) if mode == "sse" else 0,
            "versions_received": ";".join(map(str, received)),
            "version_gaps": len(gaps),
            "processing_ms": processing_ms,
            "detection_ms": detection_ms,
            "observation_db_reads": reads,
            "stream_seconds": round(stream_seconds, 3),
            "peak_active_streams": peak_streams,
            "notes": "; ".join(notes),
        })
    return output, ignored_runs, excluded_runs


def write_svg(path, title, metric, y_label, rows):
    width, height = 1000, 500
    left, right, top, bottom = 220, 55, 62, 70
    plot_width = width - left - right
    data = []
    for condition in CONDITIONS:
        for mode in METHODS:
            if metric == "observation_gets":
                key = "observation_gets" if mode == "long" else "sse_stream_gets"
            else:
                key = metric
            samples = sorted(
                [(int(row["repetition"]), float(row[key])) for row in rows
                 if row["condition"] == condition and row["mode"] == mode and row[key] != ""],
                key=lambda sample: sample[0],
            )
            data.append((condition, mode, samples))

    maximum = max((value for _, _, samples in data for _, value in samples), default=1)
    maximum = max(1.0, maximum * 1.15)
    plot_top = top
    plot_bottom = height - bottom
    plot_height = plot_bottom - plot_top

    def x(value):
        return left + plot_width * value / maximum

    parts = [
        f'<text x="{width/2}" y="30" text-anchor="middle" class="title">{title}</text>',
        f'<text x="{left + plot_width/2}" y="{height-18}" text-anchor="middle" class="axis-title">{y_label}</text>',
        f'<text x="24" y="{height/2}" text-anchor="middle" transform="rotate(-90 24 {height/2})" class="axis-title">Condition and method</text>',
    ]
    for tick in range(6):
        value = maximum * tick / 5
        tick_x = x(value)
        parts.append(f'<line x1="{tick_x:.1f}" y1="{plot_top}" x2="{tick_x:.1f}" y2="{plot_bottom}" class="grid"/>')
        parts.append(f'<text x="{tick_x:.1f}" y="{plot_bottom+20}" text-anchor="middle" class="axis">{value:.0f}</text>')

    row_height = plot_height / len(data)
    for index, (condition, mode, samples) in enumerate(data):
        center_y = plot_top + row_height * (index + 0.5)
        color = "#5364d8" if mode == "long" else "#169bab"
        method = "Long polling" if mode == "long" else "SSE"
        label = f"{condition} — {method}"
        parts.append(f'<text x="{left-12}" y="{center_y+4:.1f}" text-anchor="end" class="label">{label}</text>')
        if not samples:
            parts.append(f'<text x="{left}" y="{center_y+4:.1f}" class="axis">no run data</text>')
            continue

        values = [value for _, value in samples]
        median = statistics.median(values)
        low, high = min(values), max(values)
        parts.append(f'<line x1="{x(low):.1f}" y1="{center_y:.1f}" x2="{x(high):.1f}" y2="{center_y:.1f}" stroke="{color}" stroke-width="3" opacity=".55"/>')
        parts.append(f'<line x1="{x(low):.1f}" y1="{center_y-6:.1f}" x2="{x(low):.1f}" y2="{center_y+6:.1f}" stroke="{color}" stroke-width="2"/>')
        parts.append(f'<line x1="{x(high):.1f}" y1="{center_y-6:.1f}" x2="{x(high):.1f}" y2="{center_y+6:.1f}" stroke="{color}" stroke-width="2"/>')
        offsets = [-5, 0, 5]
        for rep_index, (repetition, value) in enumerate(samples):
            cy = center_y + offsets[rep_index % len(offsets)]
            parts.append(f'<circle cx="{x(value):.1f}" cy="{cy:.1f}" r="4.5" fill="{color}" stroke="white" stroke-width="1"><title>Repetition {repetition}: {value:g}</title></circle>')
        diamond_x = x(median)
        points = f'{diamond_x:.1f},{center_y-7:.1f} {diamond_x+7:.1f},{center_y:.1f} {diamond_x:.1f},{center_y+7:.1f} {diamond_x-7:.1f},{center_y:.1f}'
        parts.append(f'<polygon points="{points}" fill="#18212b" stroke="white" stroke-width="1"><title>Median {median:g}; range {low:g}–{high:g}</title></polygon>')

    legend_y = height - 46
    parts.extend([
        f'<circle cx="{left}" cy="{legend_y}" r="4.5" fill="#5364d8"/><text x="{left+11}" y="{legend_y+4}" class="legend">Long polling</text>',
        f'<circle cx="{left+105}" cy="{legend_y}" r="4.5" fill="#169bab"/><text x="{left+116}" y="{legend_y+4}" class="legend">SSE</text>',
        f'<circle cx="{left+178}" cy="{legend_y}" r="4.5" fill="#5364d8"/><text x="{left+190}" y="{legend_y+4}" class="legend">Individual repetition</text>',
        f'<polygon points="{left+340},{legend_y-6} {left+346},{legend_y} {left+340},{legend_y+6} {left+334},{legend_y}" fill="#18212b"/><text x="{left+352}" y="{legend_y+4}" class="legend">Median; whisker shows min–max</text>',
    ])
    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<style>text{{font-family:Arial,sans-serif;fill:#253342}}.title{{font-size:20px;font-weight:bold}}.axis-title{{font-size:13px;font-weight:bold}}.axis{{font-size:11px;fill:#536273}}.label{{font-size:12px;font-weight:bold}}.value,.legend{{font-size:11px}}.grid{{stroke:#dbe2ea;stroke-width:1}}</style><rect width="100%" height="100%" fill="white"/>{''.join(parts)}</svg>'''
    Path(path).write_text(svg, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True, help="Assessment 3 client JSONL download")
    parser.add_argument("--server", required=True, help="Assessment 3 server JSONL capture")
    parser.add_argument("--out-dir", default="experiments/v3/results")
    parser.add_argument("--exclude-job", action="append", default=[], metavar="JOB_ID_PREFIX",
                        help="leave one run out of the analysis (at least 8 characters of its job ID, repeatable); raw logs are never modified")
    args = parser.parse_args()

    client, client_skipped = read_jsonl(args.client)
    server, server_skipped = read_jsonl(args.server)
    rows, ignored_runs, excluded_runs = summarize(client, server, args.exclude_job)
    if not rows:
        raise SystemExit("No client run records found for conditions A/B/C2 and methods long/SSE.")
    output = Path(args.out_dir)
    output.mkdir(parents=True, exist_ok=True)
    with (output / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    lines = [
        "# Assessment 3 — Long polling vs SSE",
        "",
        f"Client JSON records: {len(client)} (non-JSON lines skipped: {len(client_skipped)}). Server JSON records: {len(server)} (non-JSON lines skipped: {len(server_skipped)}).",
        f"Included comparison runs: {len(rows)}. Ignored client runs outside conditions A/B/C2 and methods long/SSE: {ignored_runs}.",
        "Counts represent HTTP requests/streams, not TCP connection establishments. SSE heartbeat comments are counted from server logs; they are not job-update events.",
        "",
        *([
            "Runs excluded at the operator's request (raw logs unchanged):",
            "",
            "| Job | Condition | Method | Rep | Client events |",
            "|---|---|---|---:|---:|",
            *["| {job_id} | {condition} | {mode} | {repetition} | {events} |".format(**run) for run in excluded_runs],
            "",
        ] if excluded_runs else []),
        "## Run-by-run measurements",
        "",
        "| Run | Condition | Method | Rep | Job | Observation GETs | SSE stream GETs | Job events | Heartbeats | Versions received | Gaps | Processing (ms) | Detection (ms) | DB reads | Stream (s) | Peak streams | Notes |",
        "|---|---|---|---:|---|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append("| {run_id} | {condition} | {mode} | {repetition} | {job_id} | {observation_gets} | {sse_stream_gets} | {job_events_received} | {heartbeats} | {versions_received} | {version_gaps} | {processing_ms} | {detection_ms} | {observation_db_reads} | {stream_seconds} | {peak_active_streams} | {notes} |".format(**row))

    numeric = ["observation_gets", "sse_stream_gets", "job_events_received", "heartbeats", "version_gaps", "processing_ms", "detection_ms", "observation_db_reads", "stream_seconds", "peak_active_streams"]
    lines.extend(["", "## Median (minimum–maximum) by condition and method", "", "| Condition | Method | Observation GETs | SSE stream GETs | Job events | Heartbeats | Version gaps | Processing (ms) | Detection (ms) | DB reads | Stream (s) | Peak active streams |", "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"])
    for condition in CONDITIONS:
        for mode in METHODS:
            selected = [row for row in rows if row["condition"] == condition and row["mode"] == mode]
            cells = [median_range([row[key] for row in selected]) for key in numeric]
            lines.append(f"| {condition} | {mode} | " + " | ".join(cells) + " |")

    lines.extend(["", "## Run-count and completeness check", "", f"Observed {len(rows)} comparison runs; required is 18 (3 conditions × 2 methods × 3 repetitions)."])
    if len(rows) != len(CONDITIONS) * len(METHODS) * 3:
        lines.append(f"- Total run count is {len(rows)}; expected 18.")
    job_ids = [row["job_id"] for row in rows if row["job_id"]]
    if len(job_ids) != len(set(job_ids)):
        lines.append("- Duplicate job ID(s) found across comparison runs.")
    for condition in CONDITIONS:
        for mode in METHODS:
            selected = [row for row in rows if row["condition"] == condition and row["mode"] == mode]
            if len(selected) != 3:
                lines.append(f"- {condition}/{mode}: {len(selected)} run(s), expected 3.")
            repetitions = [int(row["repetition"]) for row in selected if str(row["repetition"]).isdigit()]
            if set(repetitions) != {1, 2, 3} or len(repetitions) != len(selected):
                lines.append(f"- {condition}/{mode}: repetitions are {sorted(repetitions)}, expected exactly [1, 2, 3].")
    notes = [row for row in rows if row["notes"]]
    lines.extend(["", f"Runs with notes requiring review: {len(notes)}.", "", "## Graphs", "", "The first graph compares per-run observation GETs for long polling with SSE stream GETs. SSE stream count is not a TCP connection count or a total resource-cost measure.", "", "![Observation GET and SSE stream counts by condition and method](observation-gets-by-method.svg)", "", "The second graph compares terminal detection delay from server commit to browser receipt. Each dot is a repetition (hover for its repetition number); the diamond is the median and the whisker is the minimum–maximum range.", "", "![Completion detection delay by condition and method](terminal-detection-delay.svg)", ""])
    (output / "summary.md").write_text("\n".join(lines), encoding="utf-8")
    write_svg(output / "observation-gets-by-method.svg", "Observation GETs and SSE streams by condition and method", "observation_gets", "HTTP GETs or opened SSE streams (count)", rows)
    write_svg(output / "terminal-detection-delay.svg", "Terminal detection delay by condition and method", "detection_ms", "Time from server commit to client receipt (ms)", rows)
    print(f"Wrote {len(rows)} Assessment 3 run rows to {output}; inspect count and notes before interpreting results.")


if __name__ == "__main__":
    main()