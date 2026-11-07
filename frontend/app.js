import { uploadImage, getJob } from './modules/data-service.js?v=14';
import {
	beginRun,
	downloadClientLog,
	requestCancelled,
	requestStarted,
	responseReceived,
	responseRendered,
	retrievalFailed,
	sseJobUpdateReceived,
	sseReconnectStarted,
	sseStreamClosed,
	sseStreamOpened,
	setRunJob,
	setRunMode,
} from './modules/experiment-logger.js?v=15';
import { AppState } from './state.js?v=15';
import { bindRetryUpload, bindTryAgain, render } from './render.js?v=15';

const form = document.querySelector('#upload-form');
const input = document.querySelector('#image-input');
const changeImageButton = document.querySelector('#change-image-button');
const modeSelect = document.querySelector('#polling-mode');
const stopObservationButton = document.querySelector('#stop-observation-button');
const downloadLogsButton = document.querySelector('#download-client-logs-button');
const runStatus = document.querySelector('#run-status');
const state = new AppState();
let observationGeneration = 0;
let activeController = null;
let activeRequestId = null;
let activeEventSource = null;
let eventSourceSequence = 0;
let shortPollTimer = null;
let shortPollResolve = null;
let requestSequence = 0;
let activeRun = null;
let sseReconnectTimer = null;
let sseRetryCount = 0;

state.on('change', nextState => {
	render(nextState);
	updateRunStatus();
	bindTryAgain(resumeObservation);
	bindRetryUpload(() => attemptUpload(state.get().file));
});
render(state.get());

input.addEventListener('change', handleFileSelection);
changeImageButton.addEventListener('click', chooseDifferentImage);
form.addEventListener('submit', event => {
	event.preventDefault();
	attemptUpload(state.get().file);
});
modeSelect.addEventListener('change', handleModeChange);
stopObservationButton.addEventListener('click', stopObserving);
downloadLogsButton.addEventListener('click', downloadClientLog);
window.addEventListener('beforeunload', () => cancelObservation('page_unload'));
for (const selector of ['#delay-ms', '#quiet-delay-ms', '#simulation-duration-ms', '#simulation-interval-ms']) {
	document.querySelector(selector)?.addEventListener('input', updateRunStatus);
}
updateRunStatus();

function getRunDelayConfig() {
	return {
		delayMs: Number(document.querySelector('#delay-ms').value || 0),
		quietDelayMs: Number(document.querySelector('#quiet-delay-ms').value || 0),
		simulationDurationMs: Number(document.querySelector('#simulation-duration-ms').value || 0),
		simulationIntervalMs: Number(document.querySelector('#simulation-interval-ms').value || 0),
	};
}

function updateRunStatus() {
	const config = getRunDelayConfig();
	const parts = [];
	if (config.delayMs > 0) parts.push(`processing ${config.delayMs}ms`);
	if (config.quietDelayMs > 0) parts.push(`quiet ${config.quietDelayMs}ms`);
	if (config.simulationDurationMs > 0 || config.simulationIntervalMs > 0) {
		parts.push(`simulated ${config.simulationDurationMs || 0}ms / ${config.simulationIntervalMs || 0}ms`);
	}
	const current = state.get();
	const statusParts = [];
	if (current.connectionStatus === 'reconnecting') {
		statusParts.push(`reconnecting (${current.retryCount || 1})`);
	} else if (current.connectionStatus === 'connecting') {
		statusParts.push('connecting');
	} else if (current.connectionStatus === 'open') {
		statusParts.push('connected');
	} else if (current.connectionStatus === 'stopped') {
		statusParts.push('stopped');
	} else if (current.connectionStatus === 'failed') {
		statusParts.push('failed');
	}
	if (current.lastError) statusParts.push(current.lastError);
	if (runStatus) {
		const runtimeStatus = parts.length ? `Custom runtime settings: ${parts.join(' • ')}` : 'Default server settings active.';
		const connectionStatus = statusParts.length ? `Connection: ${statusParts.join(' • ')}` : 'Connection: idle';
		runStatus.textContent = `${runtimeStatus} | ${connectionStatus}`;
	}
}

function validateRunDelayConfig(config) {
	if ([config.delayMs, config.quietDelayMs, config.simulationDurationMs, config.simulationIntervalMs].some(value => Number.isNaN(value) || value < 0)) {
		return 'Delay values must be zero or a positive number of milliseconds.';
	}
	if (config.simulationDurationMs > 0 && config.simulationIntervalMs <= 0) {
		return 'Simulation interval must be greater than zero when simulation duration is set.';
	}
	if (config.simulationIntervalMs > 0 && config.simulationDurationMs <= 0) {
		return 'Simulation duration must be greater than zero when simulation interval is set.';
	}
	return null;
}

function chooseDifferentImage() {
	state.update({ choosingDifferentImage: true });
	input.disabled = false;
	input.click();
}

input.addEventListener('cancel', () => {
	state.update({ choosingDifferentImage: false });
});

function handleFileSelection() {
	const file = input.files[0] || null;
	cancelObservation('file_changed');
	state.resetJob();
	if (!file) {
		state.update({ file: null, previewUrl: '', uploadError: '', choosingDifferentImage: false });
		return;
	}
	const validationError = validateFile(file);
	if (validationError) {
		state.update({ file, previewUrl: '', uploadError: validationError, choosingDifferentImage: false });
		return;
	}
	state.update({ file, previewUrl: URL.createObjectURL(file), uploadError: '', choosingDifferentImage: false });
}

async function attemptUpload(file) {
	const current = state.get();
	if (current.isSubmitting || current.job || !file || current.uploadError) return;
	const runConfig = getRunDelayConfig();
	const validationError = validateRunDelayConfig(runConfig);
	if (validationError) {
		state.update({ submitError: validationError, job: null, isSubmitting: false });
		return;
	}
	cancelObservation('new_upload');
	state.update({
		isSubmitting: true,
		job: { status: 'uploading' },
		results: [],
		resultError: '',
		submitError: '',
		paused: false,
		measurements: null,
	});
	const runSettings = {
		condition: document.querySelector('#experiment-condition').value,
		repetition: Number(document.querySelector('#experiment-repetition').value) || 1,
		image: file.name,
		latencySetting: document.querySelector('#latency-setting').value.trim() || 'not recorded',
	};

	try {
		const accepted = await uploadImage(file, runConfig);
		activeRun = beginRun({ ...runSettings, mode: state.get().mode });
		setRunJob(activeRun, accepted.job_id);
		const job = { id: accepted.job_id, statusUrl: accepted.status_url, status: accepted.status };
		state.update({ job, observing: true, paused: false, measurements: { requestsStarted: 0 } });
		void startObservation({ promptFirst: true, firstKind: 'initial' });
	} catch (error) {
		const message = error instanceof TypeError
			? 'Could not reach the image API. Make sure the Go server is running at http://localhost:4000.'
			: error.message;
		state.update({ submitError: message, job: null });
	} finally {
		state.update({ isSubmitting: false });
	}
}

function handleModeChange() {
	const mode = modeSelect.value;
	const current = state.get();
	if (current.mode === mode) return;
	const shouldResume = current.job && !isTerminal(current.job.status) && current.observing;
	// Cancel first so the cancelled request is logged under the mode it was started in.
	cancelObservation('mode_switch');
	setRunMode(activeRun, mode);
	state.update({ mode, observing: false, streamStatus: '' });
	if (shouldResume) void startObservation({ promptFirst: false });
}

function stopObserving() {
	if (!state.get().observing) return;
	cancelObservation('user_stopped');
	state.update({ observing: false, paused: true, resultError: '' });
}

function resumeObservation() {
	const job = state.get().job;
	if (!job?.statusUrl || isTerminal(job.status)) return;
	state.update({ observing: true, paused: false, resultError: '' });
	void startObservation({ promptFirst: true, firstKind: 'retry_snapshot' });
}

function cancelObservation(reason) {
	observationGeneration += 1;
	if (shortPollTimer !== null) {
		clearTimeout(shortPollTimer);
		shortPollTimer = null;
		shortPollResolve?.(false);
		shortPollResolve = null;
	}
	if (sseReconnectTimer) {
		clearTimeout(sseReconnectTimer);
		sseReconnectTimer = null;
	}
	if (activeController) {
		requestCancelled(activeRun, activeRequestId, reason);
		activeController.abort(reason);
		activeController = null;
		activeRequestId = null;
	}
	closeEventSource(reason);
	if (reason !== 'page_unload') {
		state.update({ connectionStatus: 'stopped', lastError: reason === 'user_stopped' ? 'Observation stopped by the user.' : 'Observation cancelled.', retryCount: 0 });
	}
	updateRunStatus();
}

async function startObservation({ promptFirst, firstKind = 'retry_snapshot' }) {
	cancelObservation('observation_restarted');
	const generation = observationGeneration;
	const job = state.get().job;
	if (!job?.statusUrl || isTerminal(job.status)) return;
	state.update({ observing: true, paused: false, streamStatus: '', connectionStatus: 'connecting', retryCount: 0, lastError: '', resultError: '' });

	if (state.get().mode === 'sse') {
		startSSEObservation(job, generation);
		return;
	}

	if (promptFirst) {
		const snapshot = await requestSnapshot(null, firstKind, generation);
		if (!snapshot || generation !== observationGeneration) return;
		if (snapshot.error) {
			state.update({ observing: false, resultError: 'Unable to check status.' });
			return;
		}
		if (snapshot.status === 200) {
			applySnapshot(snapshot.job, snapshot.requestId);
			if (isTerminal(snapshot.job.status)) return;
		}
	}

	while (generation === observationGeneration) {
		const current = state.get();
		if (!current.job?.statusUrl || isTerminal(current.job.status)) return;
		if (current.mode === 'short') {
			await waitForShortPoll(generation);
			if (generation !== observationGeneration) return;
		}
		const after = current.mode === 'long' ? (current.job.version ?? 0) : null;
		const result = await requestSnapshot(after, current.mode === 'long' ? 'long_poll' : 'short_poll', generation);
		if (!result || generation !== observationGeneration) return;
		if (result.error) {
			state.update({ observing: false, paused: false, resultError: 'Unable to check status.' });
			return;
		}
		if (result.status === 204) continue;
		applySnapshot(result.job, result.requestId);
		if (isTerminal(result.job.status)) return;
	}
}

function startSSEObservation(job, generation) {
	const streamUrl = new URL(job.statusUrl, window.location.origin);
	streamUrl.pathname = `${streamUrl.pathname.replace(/\/$/, '')}/events`;
	const streamId = `sse-${++eventSourceSequence}`;
	state.update({ streamStatus: 'connecting', connectionStatus: 'connecting', lastError: '', retryCount: sseRetryCount });
	let source;
	try {
		source = new EventSource(streamUrl.href);
	} catch (error) {
		state.update({ observing: false, resultError: error?.message || 'Unable to open the job event stream.', connectionStatus: 'failed', lastError: 'Unable to open the job event stream.' });
		return;
	}
	activeEventSource = { source, streamId, openedAt: null };

	source.onopen = () => {
		if (generation !== observationGeneration || activeEventSource?.source !== source) return;
		sseRetryCount = 0;
		activeEventSource.openedAt = performance.now();
		sseStreamOpened(activeRun, streamId, streamUrl.href);
		state.update({ observing: true, paused: false, streamStatus: 'open', connectionStatus: 'open', lastUpdatedAt: Date.now(), retryCount: 0, lastError: '', resultError: '' });
		updateRunStatus();
	};

	source.addEventListener('job-update', event => {
		if (generation !== observationGeneration || activeEventSource?.source !== source) return;
		const receivedAt = Date.now();
		const receivedPerfMs = performance.now();
		let snapshot;
		try {
			snapshot = JSON.parse(event.data);
		} catch {
			state.update({ streamStatus: 'reconnecting', connectionStatus: 'reconnecting', lastError: 'Received a malformed job update. Reconnecting to resume observation.', retryCount: sseRetryCount || 1 });
			return;
		}
		if (!isValidRepresentation(snapshot) || snapshot.id !== state.get().job?.id) {
			state.update({ streamStatus: 'reconnecting', connectionStatus: 'reconnecting', lastError: 'The stream returned an unexpected snapshot. Reconnecting to resume observation.', retryCount: sseRetryCount || 1 });
			return;
		}
		sseJobUpdateReceived(activeRun, streamId, snapshot, receivedAt, receivedPerfMs);
		applySnapshot(snapshot, streamId);
		if (isTerminal(snapshot.status)) closeEventSource('terminal');
	});

	source.onerror = () => {
		if (generation !== observationGeneration || activeEventSource?.source !== source) return;
		sseRetryCount += 1;
		const retryDelayMs = Math.min(1000 * (2 ** Math.min(sseRetryCount - 1, 5)), 15000);
		sseReconnectStarted(activeRun, streamId, source.readyState);
		state.update({ observing: true, streamStatus: 'reconnecting', connectionStatus: 'reconnecting', lastUpdatedAt: Date.now(), lastError: `Connection dropped. Retrying in ${Math.ceil(retryDelayMs / 1000)}s.`, retryCount: sseRetryCount });
		updateRunStatus();
		if (sseReconnectTimer) clearTimeout(sseReconnectTimer);
		sseReconnectTimer = setTimeout(() => {
			if (generation !== observationGeneration || !state.get().job || isTerminal(state.get().job.status) || state.get().mode !== 'sse') return;
			closeEventSource('retry');
			startSSEObservation(state.get().job, generation);
		}, retryDelayMs);
	};
}

function closeEventSource(reason) {
	if (!activeEventSource) return;
	const stream = activeEventSource;
	activeEventSource = null;
	stream.source.close();
	const durationMs = stream.openedAt === null ? null : performance.now() - stream.openedAt;
	sseStreamClosed(activeRun, stream.streamId, reason, durationMs);
	if (reason === 'terminal' || reason === 'user_stopped' || reason === 'mode_switch' || reason === 'observation_restarted') {
		state.update({ connectionStatus: 'stopped', lastError: reason === 'terminal' ? 'Job reached a terminal state.' : 'Observation stopped.', retryCount: 0 });
		updateRunStatus();
	}
}

function waitForShortPoll(generation) {
	return new Promise(resolve => {
		shortPollResolve = resolve;
		shortPollTimer = setTimeout(() => {
			shortPollTimer = null;
			shortPollResolve = null;
			resolve(generation === observationGeneration);
		}, 1000);
	});
}

async function requestSnapshot(after, kind, generation) {
	const current = state.get();
	const requestId = `request-${++requestSequence}`;
	const controller = new AbortController();
	activeController = controller;
	activeRequestId = requestId;
	requestStarted(activeRun, requestId, current.job.statusUrl, kind);
	const timeout = setTimeout(() => controller.abort('request_timeout_30s'), 30000);
	try {
		const response = await getJob(current.job.statusUrl, controller.signal, after);
		if (generation !== observationGeneration) return null;
		if (response.status === 204) {
			responseReceived(activeRun, requestId, 204, null, kind, response.receivedAt, response.receivedPerfMs);
			return { ...response, requestId };
		}
		if (!isValidRepresentation(response.job)) throw new Error('Invalid job representation.');
		responseReceived(activeRun, requestId, response.status, response.job, kind, response.receivedAt, response.receivedPerfMs);
		return { ...response, requestId };
	} catch (error) {
		if (generation !== observationGeneration) return null;
		// abort('request_timeout_30s') makes fetch reject with that bare string, not an Error.
		const timedOut = error === 'request_timeout_30s'
			|| (controller.signal.aborted && controller.signal.reason === 'request_timeout_30s');
		const detail = timedOut
			? 'client_timeout_30s'
			: (error?.message || error?.name || (typeof error === 'string' && error) || 'request failed');
		state.update({
			connectionStatus: timedOut ? 'failed' : 'reconnecting',
			lastUpdatedAt: Date.now(),
			lastError: timedOut
				? 'Request timed out after 30 seconds. The job is still running; observation will resume automatically.'
				: 'Status request failed. Retrying with the same job.',
			retryCount: (state.get().retryCount || 0) + 1,
		});
		retrievalFailed(activeRun, requestId, detail, error?.status || null);
		return { error, requestId };
	} finally {
		clearTimeout(timeout);
		if (activeController === controller) {
			activeController = null;
			activeRequestId = null;
		}
	}
}

function applySnapshot(job, requestId) {
	const terminal = isTerminal(job.status);
	state.update({
		job: { ...state.get().job, ...job },
		results: job.variants || [],
		observing: !terminal,
		paused: false,
		streamStatus: '',
		connectionStatus: terminal ? 'stopped' : 'open',
		lastUpdatedAt: Date.now(),
		lastError: '',
		resultError: '',
		measurements: {
			requestsStarted: activeRun?.requestsStarted || 0,
			sseStreamGets: activeRun?.sseStreamOpens || 0,
			sseEventsReceived: activeRun?.sseJobEventsReceived || 0,
			versionsSeen: activeRun?.distinctNewVersions.size || 0,
			terminalVersion: activeRun?.terminalVersion,
		},
	});
	responseRendered(activeRun, requestId, job.version);
}

function isValidRepresentation(job) {
	return job && typeof job === 'object'
		&& typeof job.id === 'string'
		&& ['queued', 'processing', 'completed', 'failed'].includes(job.status)
		&& Number.isInteger(job.version) && job.version > 0
		&& typeof job.stage === 'string'
		&& Number.isInteger(job.variants_completed) && job.variants_completed >= 0 && job.variants_completed <= 3
		&& Array.isArray(job.variants);
}

function isTerminal(status) {
	return status === 'completed' || status === 'failed';
}

function isSupportedImage(file) {
	const extension = file.name.toLowerCase().split('.').pop();
	return ['image/jpeg', 'image/png', '', 'application/octet-stream'].includes(file.type)
		&& ['jpg', 'jpeg', 'png'].includes(extension);
}

function validateFile(file) {
	if (file.size === 0) return 'The selected file is empty.';
	if (file.size > 10 * 1024 * 1024) return 'The selected file is larger than the 10 MB limit.';
	const extension = file.name.toLowerCase().split('.').pop();
	if (!['jpg', 'jpeg', 'png'].includes(extension)) return 'Unsupported file type. Choose a .jpg, .jpeg, or .png image.';
	if (!isSupportedImage(file)) return 'The selected file is not recognized as a JPEG or PNG image.';
	return '';
}