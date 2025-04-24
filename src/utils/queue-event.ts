import { Job, JobStatus } from "../types";

export interface QueueEventData {
    job: Job;
    status: JobStatus;
}

export class QueueEvent extends Event {
    constructor(type: string, public readonly job: Job, public readonly status: JobStatus) {
        super(type);
    }
}