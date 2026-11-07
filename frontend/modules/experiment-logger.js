const STORAGE_KEY = 'imagelab-assessment3-client-log-v1';
// Written into every entry so a raw log shows which frontend build produced it.
const LOGGER_BUILD = '15';

// Entries live in memory; localStorage is only written after a run ends or on page hide.
function readLog() {
	try {
		return JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]');
	} catch {
		return [];
	}
}

let entries = readLog();
let saveTimer = null;

function saveLog() {
	saveTimer = null;
	try {
		localStorage.setItem(STORAGE_KEY, JSON.stringify(entries));
	} catch (error) {
		console.warn('Could not persist the ImageLab measurement log.', error);
	}
}

function scheduleSave() {
	if (saveTimer === null) saveTimer = setTimeout(saveLog, 500);
}

window.addEventListener('pagehide', saveLog);

function append(run, event, fields = {}) {
	entries.push({
		run_id: run.id,
		condition: run.condition,
		repetition: run.repetition,
		mode: run.mode,
		job_id: run.jobId || '',
		image: run.image,
		latency_setting: run.latencySetting,
		client_build: LOGGER_BUILD,
		event,
		client_time_ms: Date.now(),
		performance_time_ms: performance.now(),
		...fields,
	});
}

export function beginRun(metadata) {
	return {
		id: globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random().toString(16).slice(2)}`,
		condition: metadata.condition,
		repetition: metadata.repetition,
		mode: metadata.mode,
		image: metadata.image,
		latencySetting: metadata.latencySetting,
		jobId: '',
		baselineVersion: null,
		lastVersion: null,
		terminalVersion: null,
		distinctNewVersions: new Set(),
		requestsStarted: 0,
		sseStreamOpens: 0,
		sseJobEventsReceived: 0,
		sseReconnectErrors: 0,
		initial200: 0,
		newer200: 0,
		unchanged200: 0,
		noChange204: 0,
		retrievalErrors: 0,
		cancellations: 0,
		pendingRequests: new Set(),
		terminalReceiptMs: null,
		processingMs: null,
		requestStartTimes: new Map(),
	};
}

export function setRunJob(run, jobId) {
	run.jobId = jobId;
	append(run, 'job_accepted');
}

export function setRunMode(run, mode) {
	if (!run || run.mode === mode) return;
	const previousMode = run.mode;
	run.mode = mode;
	append(run, 'mode_changed', { previous_mode: previousMode });
}

export function requestStarted(run, requestId, url, kind) {
	run.requestsStarted += 1;
	run.pendingRequests.add(requestId);
	const startedPerformanceMs = performance.now();
	run.requestStartTimes.set(requestId, startedPerformanceMs);
	append(run, 'request_start', { request_id: requestId, url, request_kind: kind, started_performance_ms: startedPerformanceMs });
}

export function responseReceived(run, requestId, status, job, kind, receivedAt = Date.now(), receivedPerfMs = performance.now()) {
	run.pendingRequests.delete(requestId);
	const version = Number.isInteger(job?.version) ? job.version : null;
	const startedPerformanceMs = run.requestStartTimes.get(requestId);
	const durationMs = startedPerformanceMs === undefined ? null : receivedPerfMs - startedPerformanceMs;
	let classification = 'non_200';
	let terminal = false;
	if (status === 204) {
		run.noChange204 += 1;
		classification = 'no_change_204';
	} else if (status === 200 && job) {
		if (run.baselineVersion === null) {
			run.initial200 += 1;
			run.baselineVersion = version;
			classification = 'initial_200';
		} else if (version !== null && run.lastVersion !== null && version > run.lastVersion) {
			run.newer200 += 1;
			run.distinctNewVersions.add(version);
			classification = 'newer_200';
		} else {
			run.unchanged200 += 1;
			classification = 'unchanged_200';
		}
		if (version !== null) run.lastVersion = Math.max(run.lastVersion ?? version, version);
		if (job.status === 'completed' || job.status === 'failed') {
			run.terminalVersion = version;
			run.terminalReceiptMs = receivedAt;
			terminal = true;
			const terminalAt = job.completed_at || job.failed_at;
			if (job.started_at && terminalAt) {
				run.processingMs = Date.parse(terminalAt) - Date.parse(job.started_at);
			}
		}
	}
	append(run, 'response_received', {
		request_id: requestId,
		http_status: status,
		received_version: version,
		job_status: job?.status || '',
		stage: job?.stage || '',
		variants_completed: job?.variants_completed ?? null,
		classification,
		request_kind: kind,
		request_duration_ms: durationMs,
		started_at: job?.started_at || null,
		completed_at: job?.completed_at || null,
		failed_at: job?.failed_at || null,
		client_time_ms: receivedAt,
		performance_time_ms: receivedPerfMs,
	});
	if (terminal) scheduleSave();
}

export function responseRendered(run, requestId, version) {
	append(run, 'rendered', { request_id: requestId, rendered_version: version });
}

export function sseStreamOpened(run, streamId, url) {
	if (!run) return;
	run.sseStreamOpens += 1;
	append(run, 'sse_stream_open', { stream_id: streamId, url, stream_open_number: run.sseStreamOpens });
}

export function sseJobUpdateReceived(run, streamId, job, receivedAt = Date.now(), receivedPerfMs = performance.now()) {
	if (!run) return;
	const version = Number.isInteger(job?.version) ? job.version : null;
	if (run.baselineVersion === null) {
		run.baselineVersion = version;
	} else if (version !== null && run.lastVersion !== null && version > run.lastVersion) {
		run.distinctNewVersions.add(version);
	}
	if (version !== null) run.lastVersion = Math.max(run.lastVersion ?? version, version);
	run.sseJobEventsReceived += 1;
	const terminal = job?.status === 'completed' || job?.status === 'failed';
	if (terminal) {
		run.terminalVersion = version;
		run.terminalReceiptMs = receivedAt;
		const terminalAt = job.completed_at || job.failed_at;
		if (job.started_at && terminalAt) run.processingMs = Date.parse(terminalAt) - Date.parse(job.started_at);
	}
	append(run, 'sse_job_update_received', {
		stream_id: streamId,
		received_version: version,
		job_status: job?.status || '',
		stage: job?.stage || '',
		variants_completed: job?.variants_completed ?? null,
		simulation_step: job?.simulation_step ?? 0,
		started_at: job?.started_at || null,
		completed_at: job?.completed_at || null,
		failed_at: job?.failed_at || null,
		client_time_ms: receivedAt,
		performance_time_ms: receivedPerfMs,
	});
	if (terminal) scheduleSave();
}

export function sseReconnectStarted(run, streamId, readyState) {
	if (!run) return;
	run.sseReconnectErrors += 1;
	append(run, 'sse_reconnecting', { stream_id: streamId, ready_state: readyState });
	scheduleSave();
}

export function sseStreamClosed(run, streamId, reason, durationMs = null) {
	if (!run) return;
	append(run, 'sse_client_stream_closed', { stream_id: streamId, reason, duration_ms: durationMs });
	scheduleSave();
}

export function retrievalFailed(run, requestId, detail, httpStatus = null) {
	run.pendingRequests.delete(requestId);
	run.retrievalErrors += 1;
	const startedPerformanceMs = run.requestStartTimes.get(requestId);
	append(run, 'retrieval_error', {
		request_id: requestId,
		detail: String(detail),
		http_status: httpStatus,
		request_duration_ms: startedPerformanceMs === undefined ? null : performance.now() - startedPerformanceMs,
	});
	scheduleSave();
}

export function requestCancelled(run, requestId, reason) {
	if (!run.pendingRequests.has(requestId)) return;
	run.pendingRequests.delete(requestId);
	run.cancellations += 1;
	const startedPerformanceMs = run.requestStartTimes.get(requestId);
	append(run, 'request_cancelled', {
		request_id: requestId,
		reason,
		request_duration_ms: startedPerformanceMs === undefined ? null : performance.now() - startedPerformanceMs,
	});
}

export function downloadClientLog() {
	saveLog();
	const contents = entries.map(entry => JSON.stringify(entry)).join('\n');
	const blob = new Blob([contents ? `${contents}\n` : ''], { type: 'application/x-ndjson' });
	const url = URL.createObjectURL(blob);
	const anchor = document.createElement('a');
	anchor.href = url;
	anchor.download = 'imagelab-client-observations.jsonl';
	anchor.click();
	URL.revokeObjectURL(url);
}