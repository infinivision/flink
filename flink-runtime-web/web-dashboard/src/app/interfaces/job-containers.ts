export interface Container {
    cid: string;
    node: string;
    logURL: string;
}

export interface Attempt {
    cid: string;
    attemptID: string;
    host: string;
    state: string;
    trackingURL: string;
    flinkWeb: string;
    containers: Container[];
}

export interface AttempRequestInterface {
    'containers': Attempt[];
}
