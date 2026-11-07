const acceptedStatus = 202;

export async function uploadImage(file, runConfig = {}) {
	const body = new FormData();
	body.append('image', file);
	body.append('delay_ms', String(runConfig.delayMs ?? 0));
	body.append('quiet_delay_ms', String(runConfig.quietDelayMs ?? 0));
	body.append('simulation_duration_ms', String(runConfig.simulationDurationMs ?? 0));
	body.append('simulation_interval_ms', String(runConfig.simulationIntervalMs ?? 0));

	const response = await fetch('/v1/images', { method: 'POST', body });
	const data = await readJson(response);
	if (!response.ok || response.status !== acceptedStatus) {
		throw new Error(data.error || 'The image could not be accepted.');
	}

	return data;
}

export async function getJob(statusUrl, signal, after = null) {
	const url = new URL(statusUrl, window.location.origin);
	if (after !== null) url.searchParams.set('after', String(after));
	const response = await fetch(url, { signal, cache: 'no-store' });
	if (response.status === 204) {
		return { status: 204, job: null, receivedAt: Date.now(), receivedPerfMs: performance.now() };
	}
	if (!response.ok) {
		const error = new Error(`Status request failed with HTTP ${response.status}.`);
		error.status = response.status;
		throw error;
	}
	try {
		const job = await response.json();
		// Stamp receipt as soon as the full representation is read, before any logging work.
		return { status: response.status, job, receivedAt: Date.now(), receivedPerfMs: performance.now() };
	} catch {
		throw new Error('The status response was not valid JSON.');
	}
}

async function readJson(response) {
	return response.json().catch(() => ({}));
}