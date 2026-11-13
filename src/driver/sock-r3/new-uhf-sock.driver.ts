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
import { MessageProcessing } from "./utils/message-procesing";

export class UHFSockDriverV2 implements BaseDriver {
    public name: Drivers = Drivers.UHF_SOCKET_R3_V2;
    public isRunning: boolean = false;
    private driverInfo: R3DriverInfo;
    private static instance: UHFSockDriverV2 | null = null;
    // utils
    private retryAttempts = 0;
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
    private emitError(message: string) {
        this.subject.next(
            new Message(SockEvent.ERROR, new UHFSocketError(message)),
        );
    }
    private retryConnection() {
        if (this.retryAttempts < 5) {
            this.retryAttempts++;
            setTimeout(() => {
                this.start();
            }, 1000 * this.retryAttempts);
        } else {
            this.stop();
            this.emitError("Max retry attempts reached. Connection failed.");
        }
    }
    private sendMessage<T>(message: Message<T>) {
        this.client.write(message.toJson() + "\n");
    }
    // interface methods
    public async start(): Promise<void> {
        this.subject = new Subject<Message>();
        this._client = createConnection(this.driverInfo.socketPath, () =>
            this.emit(SockEvent.CONNECTED, null),
        );
        this.client.on("data", (data) => {
            MessageProcessing.create(this.client, this.subject).processMessage(
                data,
                () => (this.retryAttempts = 0),
            );
        });
        this.client.on("close", () => {
            this.log("Socket closed.");
            this.stop();
        });

        this.client.on("error", (err) => {
            this.emitError("Socket error: " + String(err));
            this.log("Socket error: " + String(err));
            this.retryConnection();
        });

        this.client.on("end", () => {
            this.log("Socket ended.");
            this.stop();
        });

        return Promise.resolve();
    }
    public stop() {
        if (this._client) {
            this.emit(SockEvent.DISCONNECTED, null);
            this.killProcess();
        }
        return Promise.resolve();
    }
    public send<K extends SendSockEvent>(event: K, data: SendEventMap[K]) {
        const message = new Message(event as unknown as SockEvent, data);
        this.sendMessage(message);
    }
    public on<K extends SockEvent>(
        event: K,
        callback: EventMap[K],
    ): Subscription {
        const subscription: Subscription = this.observable.subscribe(
            (message) => {
                if (message.event === event) {
                    callback(message as any);
                }
            },
        );
        return subscription;
    }
    public onAll(callback: (message: Message) => void) {
        const subscription: Subscription = this.observable.subscribe(
            (message) => {
                callback(message);
            },
        );
        return subscription;
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
