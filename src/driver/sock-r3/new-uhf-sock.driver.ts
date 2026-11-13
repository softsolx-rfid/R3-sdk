import { EventMap } from "@/@types/event-map";
import { SendEventMap } from "@/@types/send-event-map";
import { Drivers, UHFSocketError } from "@/index";
import { Message } from "@/messages/dto/message";
import { Subscription } from "rxjs/internal/Subscription";
import { BaseDriver } from "../base-driver.abstract";
import { SockEvent } from "../events.enum";
import { SendSockEvent } from "../send-events.enum";
import { R3DriverInfo } from "./utils/driver-info";
import { createConnection, Socket } from "net";
import { Subject } from "rxjs";
import { KillProcess } from "./utils/kill-process";
import { GetUHFSocketLogs } from "./utils/get-uhf-logs";
import { writeLog } from "./utils/write-log";

export class UHFSockDriverV2 implements BaseDriver {
    public name: Drivers = Drivers.UHF_SOCKET_R3_V2;
    public isRunning: boolean = false;
    private driverInfo: R3DriverInfo;
    private static instance: UHFSockDriverV2 | null = null;

    private subject = new Subject<Message>();
    public get observable() {
        return this.subject.asObservable();
    }

    private _client: Socket | null = null;
    private get client(): Socket {
        if (!this._client) {
            throw new UHFSocketError("Client not initialized.");
        }
        return this._client;
    }

    constructor() {
        if (!UHFSockDriverV2.instance) {
            this.driverInfo = new R3DriverInfo();
            UHFSockDriverV2.instance = this;
        } else {
            throw new UHFSocketError("Instance already exists.");
        }
    }

    // locals methods
    private emit<T extends SockEvent>(event: T, data: any) {
        this.subject.next(new Message(event, data));
    }

    // interface methods

    public async start(): Promise<void> {
        this.subject = new Subject<Message>();
        this._client = createConnection(this.driverInfo.socketPath, () =>
            this.emit(SockEvent.CONNECTED, null),
        );
    }
    public stop(): Promise<void> {
        throw new Error("Method not implemented.");
    }
    public send<K extends SendSockEvent>(
        event: K,
        data: SendEventMap[K],
    ): void {
        throw new Error("Method not implemented.");
    }
    public on<K extends SockEvent>(
        event: K,
        callback: EventMap[K],
    ): Subscription {
        throw new Error("Method not implemented.");
    }
    public onAll(callback: (message: Message) => void): Subscription {
        throw new Error("Method not implemented.");
    }

    public async sendRaw(data: string) {
        this.client.write(data);
    }
    public killProcess() {
        this.subject.complete();
        if (this._client) {
            this.client.end();
            this.client.destroy();
            this._client = null;
        }
        UHFSockDriverV2.instance = null;
        KillProcess.kill(this.driverInfo.pid);
        this.subject.next(new Message(SockEvent.EXIT, null));
    }
    public async getLogs(maxLines: number): Promise<string> {
        return GetUHFSocketLogs.start(this.driverInfo.logsPath, maxLines);
    }
    public async log(...logs: string[]): Promise<void> {
        await writeLog(this.driverInfo.logsPath, ...logs);
    }
}
