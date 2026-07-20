import * as rxjs from 'rxjs';
import { Subscription } from 'rxjs';

declare enum SockEvent {
    ERROR = "ERROR",
    CONNECTED = "CONNECTED",
    DISCONNECTED = "DISCONNECTED",
    START = "START",
    STOP = "STOP",
    RESET = "RESET",
    SET_POWER = "SET_POWER",
    GET_POWER = "GET_POWER",
    SET_BEEP = "SET_BEEP",
    GET_BEEP = "GET_BEEP",
    TAG = "TAG",
    TAG_RAW = "TAG_RAW",
    EXIT = "EXIT"
}

declare class Message<T = any> {
    event: SockEvent;
    data: T;
    constructor(event: SockEvent, data: T);
    toJson(): string;
}

declare class ErrorMessage extends Message<string> {
    constructor(data: string);
    static create(data: string): ErrorMessage;
}

declare class GetPowerMessage extends Message<Array<number>> {
    constructor(s: Array<number>);
    static create(s: Array<number>): GetPowerMessage;
}

type EventMap = {
    [SockEvent.CONNECTED]: (message: Message<string>) => void;
    [SockEvent.DISCONNECTED]: (message: Message<string>) => void;
    [SockEvent.TAG]: (message: Message<string>) => void;
    [SockEvent.ERROR]: (message: ErrorMessage) => void;
    [SockEvent.START]: (message: Message<string>) => void;
    [SockEvent.STOP]: (message: Message<string>) => void;
    [SockEvent.RESET]: (message: Message<string>) => void;
    [SockEvent.SET_POWER]: (message: Message<string>) => void;
    [SockEvent.GET_POWER]: (message: GetPowerMessage) => void;
    [SockEvent.SET_BEEP]: (message: Message<string>) => void;
    [SockEvent.GET_BEEP]: (message: Message<boolean>) => void;
    [SockEvent.TAG_RAW]: (message: Message<{
        epc: string;
        antenna: string;
        frequency: string;
        user: string;
    }>) => void;
    [SockEvent.EXIT]: (message: Message<null>) => void;
};

declare enum SendSockEvent {
    DISCONNECTED = "DISCONNECTED",
    START = "START",
    STOP = "STOP",
    RESET = "RESET",
    SET_POWER = "SET_POWER",
    GET_POWER = "GET_POWER",
    SET_BEEP = "SET_BEEP",
    GET_BEEP = "GET_BEEP",
    EXIT = "EXIT"
}

declare enum Antenna {
    ALL = 0,
    ANTENNA_1 = 1,
    ANTENNA_2 = 2,
    ANTENNA_3 = 3,
    ANTENNA_4 = 4
}

type SendEventMap = {
    [SendSockEvent.DISCONNECTED]: null;
    [SendSockEvent.START]: null;
    [SendSockEvent.STOP]: null;
    [SendSockEvent.RESET]: null;
    [SendSockEvent.SET_POWER]: {
        power: number;
        antenna: Antenna;
    };
    [SendSockEvent.GET_POWER]: null;
    [SendSockEvent.SET_BEEP]: boolean;
    [SendSockEvent.GET_BEEP]: null;
    [SendSockEvent.EXIT]: null;
};

declare class UHFSocketSetup {
    beep: boolean;
    power: number;
    constructor(beep: boolean, power: number);
}

declare class UHFSocketError extends Error {
    constructor(message: string);
}

declare class UhfSocket {
    private setup;
    private connection;
    private static subscriptions;
    private static instance;
    private static started;
    constructor(setup: UHFSocketSetup);
    inicialice(): void;
    stop(): void;
    get observable(): rxjs.Observable<Message<any>>;
    send<K extends SendSockEvent>(event: K, data: SendEventMap[K]): void;
    on<K extends SockEvent>(event: K, callback: EventMap[K]): Subscription;
    onAll(callback: (message: Message) => void): Subscription;
    killProcess(): void;
    getLogs(maxLines?: number): Promise<string>;
}

export { Antenna, SendSockEvent, SockEvent, UHFSocketError, UhfSocket as default };
