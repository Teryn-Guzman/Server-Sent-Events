const elements = {
	button: document.querySelector('#process-button'),
	buttonLabel: document.querySelector('#process-button .button-label'),
	input: document.querySelector('#image-input'),
	dropzone: document.querySelector('.dropzone'),
	changeImageButton: document.querySelector('#change-image-button'),
	selection: document.querySelector('#selection'),
	preview: document.querySelector('#preview'),
	filename: document.querySelector('#filename'),
	fileMeta: document.querySelector('#file-meta'),
	uploadError: document.querySelector('#upload-error'),
	jobCard: document.querySelector('#job-card'),
	results: document.querySelector('#results'),
	resultCount: document.querySelector('#result-count'),
	modeSelect: document.querySelector('#polling-mode'),
	stopObservationButton: document.querySelector('#stop-observation-button'),
};

// Fixed variant contract so pending slots can render before the worker finishes.
const VARIANT_SLOTS = [
	{ name: 'thumbnail', label: 'Thumbnail', target: '150 × 150' },
	{ name: 'preview', label: 'Preview', target: 'Max 800 × 600' },
	{ name: 'display', label: 'Display', target: 'Max 1200 × 900' },
];

const STATUS_LABELS = {
	uploading: 'Uploading',
	queued: 'Queued',
	processing: 'Processing',
	completed: 'Completed',
	failed: 'Failed',
};

export function render(state) {
	elements.modeSelect.value = state.mode || 'short';
	elements.stopObservationButton.disabled = !state.observing;
	renderSelection(state);
	renderUploadState(state);
	renderJob(state);
	renderResults(state);
}

/* ---------- upload panel ---------- */

function renderSelection(state) {
	const { file, previewUrl } = state;
	const hasValidFile = Boolean(file) && !state.uploadError;
	const pickerHidden = state.isSubmitting || (hasValidFile && !state.choosingDifferentImage);
	const showChangeButton = hasValidFile || state.choosingDifferentImage;

	elements.uploadError.textContent = state.uploadError;
	elements.input.disabled = state.isSubmitting || (hasValidFile && !state.choosingDifferentImage);
	elements.dropzone.classList.toggle('hidden', pickerHidden);
	elements.selection.classList.toggle('hidden', !hasValidFile);
	elements.changeImageButton.classList.toggle('hidden', !showChangeButton);
	elements.changeImageButton.disabled = state.isSubmitting;

	if (!hasValidFile || !previewUrl) return;

	elements.preview.src = previewUrl;
	elements.filename.textContent = file.name;
	elements.fileMeta.textContent = `${formatBytes(file.size)} · ${mediaTypeLabel(file)}`;
}

function renderUploadState(state) {
	const status = state.job?.status;
	elements.button.disabled =
		state.isSubmitting || Boolean(state.job) || !state.file || Boolean(state.uploadError);

	if (state.isSubmitting) {
		elements.buttonLabel.textContent = 'Uploading...';
		return;
	}
	if (status === 'completed') {
		elements.buttonLabel.textContent = 'Image processed';
		return;
	}
	if (status === 'failed') {
		elements.buttonLabel.textContent = 'Processing failed';
		return;
	}
	elements.buttonLabel.textContent = state.job ? 'Processing image...' : 'Process image';
}

/* ---------- job panel ---------- */

function renderJob(state) {
	const job = state.job;

	// A submission failure (e.g. lost connection) means no job was ever
	// created, so retrying just resubmits the same file rather than resuming
	// observation of an existing one.
	if (!job && state.submitError) {
		elements.jobCard.className = 'job-body empty-state is-failed';
		elements.jobCard.innerHTML = `${emptyMarkup('Upload failed', escapeHTML(state.submitError))}<button class="try-again" type="button" data-action="retry-upload">Retry upload</button>`;
		return;
	}

	if (!job) {
		elements.jobCard.className = 'job-body empty-state';
		elements.jobCard.innerHTML = emptyMarkup('No active job', 'Submit an image to begin.');
		return;
	}

	const status = state.resultError ? 'unknown' : job.status || 'queued';
	const failed = job.status === 'failed';
	const shortId = job.id ? job.id.split('-')[0] : '';

	elements.jobCard.className = `job-body${failed ? ' is-failed' : ''}`;
	elements.jobCard.innerHTML = `
		<div class="job-head">
			<div class="job-identity">
				<p class="job-label">Job ID</p>
				<p class="job-id" title="${escapeHTML(job.id || '')}">${shortId ? `#${escapeHTML(shortId)}` : 'Pending acceptance'}</p>
			</div>
			<span class="badge badge-${status}"><i class="badge-dot"></i>${status === 'unknown' ? 'Not checked' : STATUS_LABELS[job.status] || 'Queued'}</span>
		</div>
		<div class="job-observation-meta">
			<span><strong>Mode</strong> ${modeLabel(state.mode)}</span>
			<span><strong>Version</strong> ${Number.isInteger(job.version) ? job.version : '—'}</span>
		</div>
		<div class="job-split">
			${timelineHTML(job)}
			${asideHTML(state, job)}
		</div>
		${progressHTML(job)}
		${measurementsHTML(state, job)}
	`;
}

function progressHTML(job) {
	const completed = Number.isInteger(job.variants_completed) ? job.variants_completed : 0;
	const stage = job.stage ? job.stage.replaceAll('_', ' ') : 'starting';
	const simulated = Number(job.simulation_step) > 0
		? `<span class="simulated-progress">Simulated progress: step ${Number(job.simulation_step)}</span>`
		: '';
	return `<section class="job-progress" aria-label="Image processing progress">
		<div class="progress-heading"><strong>Real variants</strong><span>${completed} of 3 stored</span></div>
		<progress max="3" value="${completed}" aria-label="${completed} of 3 real variants stored"></progress>
		<span class="progress-stage">Stage: ${escapeHTML(stage)}</span>${simulated}
	</section>`;
}

// Required job lifecycle: accepted, stored, generating, complete.
// A failure marks the step it stopped at plus the final step.
function timelineHTML(job) {
	const status = job.status;
	const accepted = status !== 'uploading';
	const failed = status === 'failed';

	const steps = [
		{ label: 'Upload accepted', time: job.queued_at, state: accepted ? 'done' : 'current' },
		{ label: 'Original stored', time: job.queued_at, state: accepted ? 'done' : 'pending' },
		{
			label: 'Generating variants',
			time: job.started_at,
			state: status === 'processing' ? 'current' : status === 'completed' ? 'done' : failed ? 'failed' : 'pending',
		},
		{
			label: failed ? 'Failed' : 'Complete',
			time: failed ? job.failed_at : job.completed_at,
			state: status === 'completed' ? 'done' : failed ? 'failed' : 'pending',
		},
	];

	return `<ol class="timeline">${steps
		.map(
			step => `
		<li class="step is-${step.state}">
			<span class="step-marker"></span>
			<div class="step-text"><strong>${step.label}</strong><span class="step-time">${formatTime(step.time) || pendingLabel(step.state)}</span></div>
		</li>`,
		)
		.join('')}</ol>`;
}

// Retrieval error != processing failure — preserve state and offer a retry.
function asideHTML(state, job) {
	if (state.resultError) {
		return `<div class="job-aside">
			<strong class="aside-title">Unable to check status</strong>
			<span class="aside-note">The job may still be running.</span>
			<button class="try-again" type="button" data-action="try-again">Try again</button>
		</div>`;
	}
	if (state.paused) {
		return `<div class="job-aside">
			<strong class="aside-title">Observation stopped</strong>
			<span class="aside-note">Image processing continues on the server.</span>
			<button class="try-again" type="button" data-action="try-again">Resume observation</button>
		</div>`;
	}
	if (state.connectionStatus === 'reconnecting' || state.streamStatus === 'reconnecting') {
		const retrySuffix = Number.isInteger(state.retryCount) && state.retryCount > 0 ? ` Retry ${state.retryCount}.` : '';
		return `<div class="job-aside">
			<strong class="aside-title">Reconnecting to job stream</strong>
			<span class="aside-note">The job continues on the server.${retrySuffix} The next snapshot will resume observation automatically when the stream reconnects.</span>
			${state.lastError ? `<span class="aside-note job-error">${escapeHTML(state.lastError)}</span>` : ''}
		</div>`;
	}
	if (state.connectionStatus === 'connecting' || state.streamStatus === 'connecting') {
		return `<div class="job-aside">
			<span class="polling-dot"></span>
			<strong class="aside-title">Connecting to job stream</strong>
			<span class="aside-note">The server will send the current snapshot when the stream opens.</span>
		</div>`;
	}
	if (state.connectionStatus === 'stopped' && !job.status) {
		return `<div class="job-aside">
			<strong class="aside-title">Observation stopped</strong>
			<span class="aside-note">The job is still running on the server. Resume to continue watching it.</span>
		</div>`;
	}
	if (job.status === 'completed') {
		return `<div class="job-aside">
			<strong class="aside-title is-good">Processing complete</strong>
			<span class="aside-note">All three variants are ready.</span>
		</div>`;
	}
	if (job.status === 'failed') {
		return `<div class="job-aside">
			<strong class="aside-title is-bad">Processing failed</strong>
			<span class="aside-note job-error">${escapeHTML(job.error || 'The image could not be processed.')}</span>
		</div>`;
	}
	const title = state.mode === 'sse'
		? 'Listening for job updates'
		: state.mode === 'long' ? 'Waiting for a newer version' : 'Checking status automatically';
	const note = state.mode === 'sse'
		? 'One open event stream sends complete snapshots when the job changes.'
		: state.mode === 'long' ? 'Held request; next request follows each response.' : 'Short poll about every 1 second after each response.';
	return `<div class="job-aside">
		<span class="polling-dot"></span>
		<strong class="aside-title">${title}</strong>
		<span class="aside-note">${note}</span>
		${job.status === 'queued' && job.queue_position ? `<span class="aside-note">Queue position: ${Number(job.queue_position)}</span>` : ''}
	</div>`;
}

function measurementsHTML(state, job) {
	const measurements = state.measurements;
	if (!measurements || job.status === 'uploading') return '';
	const completedAt = job.completed_at || job.failed_at;
	const processingMs = job.started_at && completedAt ? Date.parse(completedAt) - Date.parse(job.started_at) : null;
	const rows = [
		['Observation GETs started', String(measurements.requestsStarted ?? 0)],
		['SSE stream GETs opened', String(measurements.sseStreamGets ?? 0)],
		['SSE job-update events', String(measurements.sseEventsReceived ?? 0)],
		['Distinct newer versions seen', String(measurements.versionsSeen ?? 0)],
		['Terminal version', String(measurements.terminalVersion ?? '—')],
		['Processing duration (DB timestamps)', formatDuration(processingMs)],
	].filter(([, value]) => value !== '—');
	return `<section class="job-measurements" aria-label="Job measurements">
		<strong>Observation snapshot</strong>
		<dl>${rows.map(([label, value]) => `<div><dt>${label}</dt><dd>${escapeHTML(value)}</dd></div>`).join('')}</dl>
		<span class="measurement-note">Compute detection delay by joining the client receipt log to the server post-commit log.</span>
	</section>`;
}

function modeLabel(mode) {
	if (mode === 'sse') return 'Server-Sent Events (SSE)';
	if (mode === 'long') return 'Long polling';
	return 'Short polling';
}

/* ---------- results panel ---------- */

// UI-14/15, IMG-03: results stay hidden until the job is fully completed.
function renderResults(state) {
	const job = state.job;

	if (!job || job.status === 'uploading') {
		elements.resultCount.textContent = '0 files';
		elements.results.className = 'results empty-state';
		elements.results.innerHTML = emptyMarkup('No images generated yet', 'Completed variants will appear here.');
		return;
	}

	if (job.status === 'failed') {
		elements.resultCount.textContent = '0 files';
		elements.results.className = 'results empty-state is-failed';
		elements.results.innerHTML = emptyMarkup('No images generated', 'This job failed, so no variants are available.');
		return;
	}

	if (job.status !== 'completed') {
		elements.resultCount.textContent = '0 files';
		elements.results.className = 'results';
		elements.results.innerHTML = `
			<p class="results-notice"><strong>Images are being generated.</strong> Results will appear when processing completes.</p>
			${VARIANT_SLOTS.map(pendingCardHTML).join('')}
		`;
		return;
	}

	const variants = Array.isArray(state.results)
		? state.results.filter(variant => variant?.url && variant?.name)
		: [];

	if (!variants.length) {
		elements.resultCount.textContent = '0 files';
		elements.results.className = 'results empty-state';
		elements.results.innerHTML = emptyMarkup('No generated variants were returned', 'The job completed without result metadata.');
		return;
	}

	elements.resultCount.textContent = `${variants.length} files`;
	elements.results.className = 'results';
	elements.results.innerHTML = variants.map(readyCardHTML).join('');
}

function variantDimensionLabel(name, width, height) {
	if (name === 'thumbnail') return '150 × 150';
	if (name === 'preview') return `Max 800 × 600 (${Number(width) || 0} × ${Number(height) || 0})`;
	if (name === 'display') return `Max 1200 × 900 (${Number(width) || 0} × ${Number(height) || 0})`;
	return `${Number(width) || 0} × ${Number(height) || 0}`;
}

function readyCardHTML(variant) {
	return `<article class="result">
		<div class="result-frame">
			<img src="${escapeHTML(variant.url)}" alt="${escapeHTML(variant.name)} variant" loading="lazy">
			<span class="pill pill-ready"><i class="pill-dot"></i>Ready</span>
		</div>
		<div class="result-meta">
			<div><strong>${capitalize(variant.name)}</strong><span>${variantDimensionLabel(variant.name, variant.width, variant.height)}</span></div>
			<a class="download" href="${escapeHTML(variant.url)}" target="_blank" rel="noreferrer">View / download</a>
		</div>
	</article>`;
}

function pendingCardHTML(slot) {
	return `<article class="result is-pending">
		<div class="result-frame">
			<div class="result-placeholder"></div>
			<span class="pill pill-pending"><i class="pill-dot"></i>Pending</span>
		</div>
		<div class="result-meta">
			<div><strong>${slot.label}</strong><span>${slot.target}</span></div>
		</div>
	</article>`;
}

/* ---------- helpers ---------- */

export function bindTryAgain(listener) {
	elements.jobCard.querySelector('[data-action="try-again"]')?.addEventListener('click', listener);
}

export function bindRetryUpload(listener) {
	elements.jobCard.querySelector('[data-action="retry-upload"]')?.addEventListener('click', listener);
}

function emptyMarkup(title, subtitle) {
	return `<strong>${title}</strong><span>${subtitle}</span>`;
}

function formatTime(value) {
	if (!value) return '';
	const date = new Date(value);
	return Number.isNaN(date.getTime())
		? ''
		: date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function formatDuration(value) {
	if (!Number.isFinite(value) || value < 0) return '—';
	return value < 1000 ? `${Math.round(value)} ms` : `${(value / 1000).toFixed(2)} s`;
}

function pendingLabel(state) {
	if (state === 'current') return 'In progress';
	if (state === 'failed') return 'Failed';
	return 'Pending';
}

function capitalize(value) {
	const text = String(value);
	return text.charAt(0).toUpperCase() + text.slice(1);
}

function formatBytes(bytes) {
	const exactBytes = new Intl.NumberFormat().format(bytes);
	if (bytes < 1024) return `${exactBytes} bytes`;
	if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KiB (${exactBytes} bytes)`;
	return `${(bytes / (1024 * 1024)).toFixed(2)} MiB (${exactBytes} bytes)`;
}

function mediaTypeLabel(file) {
	if (file.type === 'image/jpeg') return 'JPEG';
	if (file.type === 'image/png') return 'PNG';
	return file.type || 'type not reported';
}

// Untrusted values are always escaped before going into innerHTML.
function escapeHTML(value) {
	return String(value).replace(
		/[&<>"']/g,
		character => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[character],
	);
}