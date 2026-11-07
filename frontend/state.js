import { EventEmitter } from './modules/event-emitter.js';

const initialState = {
	file: null,
	previewUrl: '',
	uploadError: '',
	submitError: '',
	isSubmitting: false,
	job: null,
	results: [],
	resultError: '',
	observing: false,
	paused: false,
	streamStatus: '',
	mode: 'short',
	measurements: null,
	choosingDifferentImage: false,
};

export class AppState extends EventEmitter {
	constructor() {
		super();
		this.value = { ...initialState };
	}

	get() {
		return this.value;
	}

	update(changes) {
		this.value = { ...this.value, ...changes };
		this.emit('change', this.value);
	}

	resetJob() {
		this.update({ job: null, results: [], resultError: '', submitError: '', observing: false, paused: false, streamStatus: '', measurements: null });
	}
}