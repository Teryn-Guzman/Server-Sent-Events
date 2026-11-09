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

state.on('change', nextState => {
	render(nextState);
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
		const accepted = await uploadImage(file);
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
	// Only log a mode change on a run that is still active. After a run ends (or
	// before the first upload) the dropdown just sets the mode for the next run.
	if (current.job?.id && !isTerminal(current.job.status)) setRunMode(activeRun, mode);
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
	if (activeController) {
		requestCancelled(activeRun, activeRequestId, reason);
		activeController.abort(reason);
		activeController = null;
		activeRequestId = null;
	}
	closeEventSource(reason);
}

async function startObservation({ promptFirst, firstKind = 'retry_snapshot' }) {
	cancelObservation('observation_restarted');
	const generation = observationGeneration;
	const job = state.get().job;
	if (!job?.statusUrl || isTerminal(job.status)) return;
	state.update({ observing: true, paused: false, streamStatus: '', resultError: '' });

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
	state.update({ streamStatus: 'connecting' });
	let source;
	try {
		source = new EventSource(streamUrl.href);
	} catch (error) {
		state.update({ observing: false, resultError: error?.message || 'Unable to open the job event stream.' });
		return;
	}
	activeEventSource = { source, streamId, openedAt: null };

	source.onopen = () => {
		if (generation !== observationGeneration || activeEventSource?.source !== source) return;
		activeEventSource.openedAt = performance.now();
		sseStreamOpened(activeRun, streamId, streamUrl.href);
		state.update({ observing: true, paused: false, streamStatus: 'open', resultError: '' });
	};

	source.addEventListener('job-update', event => {
		if (generation !== observationGeneration || activeEventSource?.source !== source) return;
		const receivedAt = Date.now();
		const receivedPerfMs = performance.now();
		let snapshot;
		try {
			snapshot = JSON.parse(event.data);
		} catch {
			state.update({ streamStatus: 'reconnecting' });
			return;
		}
		if (!isValidRepresentation(snapshot) || snapshot.id !== state.get().job?.id) {
			state.update({ streamStatus: 'reconnecting' });
			return;
		}
		sseJobUpdateReceived(activeRun, streamId, snapshot, receivedAt, receivedPerfMs);
		applySnapshot(snapshot, streamId);
		if (isTerminal(snapshot.status)) closeEventSource('terminal');
	});

	source.onerror = () => {
		if (generation !== observationGeneration || activeEventSource?.source !== source) return;
		sseReconnectStarted(activeRun, streamId, source.readyState);
		if (source.readyState === EventSource.CLOSED) {
			// The browser has given up and will not retry (for example a reconnect got
			// a 404 or 500). That is an observation failure, not a job failure: keep the
			// last snapshot and offer Try again, which opens one new stream for the same job.
			closeEventSource('connection_closed');
			state.update({ observing: false, paused: false, streamStatus: '', resultError: 'Unable to check status.' });
			return;
		}
		// CONNECTING: keep the same EventSource so the browser reconnects by itself.
		state.update({ observing: true, streamStatus: 'reconnecting' });
	};
}

function closeEventSource(reason) {
	if (!activeEventSource) return;
	const stream = activeEventSource;
	activeEventSource = null;
	stream.source.close();
	const durationMs = stream.openedAt === null ? null : performance.now() - stream.openedAt;
	sseStreamClosed(activeRun, stream.streamId, reason, durationMs);
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