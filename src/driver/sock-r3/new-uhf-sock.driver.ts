import { EventMap } from "@/@types/event-map";
import { SendEventMap } from "@/@types/send-event-map";
import { Drivers, UHFSocketError } from "@/index";
import { Message } from "@/messages/dto/message";
import { Subscription } from "rxjs";
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
    private driverInfo: R3DriverInfo;
    private static instance: UHFSockDriverV2 | null = null;
    // utils
    private retryAttempts = 0;
    private retryTimer: NodeJS.Timeout | null = null;
    private subject = new Subject<Message>();
    public get isRunning(): boolean {
        return this._client !== null;
    }

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
        if (this.retryTimer) {
            return;
        }
        if (this.retryAttempts < 5) {
            this.retryAttempts++;
            this.retryTimer = setTimeout(() => {
                this.retryTimer = null;
                const client = this._client;
                this._client = null;
                if (client) {
                    client.destroy();
                }
                void this.start();
            }, 1000 * this.retryAttempts);
        } else {
            this.emitError("Max retry attempts reached. Connection failed.");
            void this.stop();
        }
    }
    private sendMessage<T>(message: Message<T>) {
        this.client.write(message.toJson() + "\n");
    }
    // interface methods
    public async start(): Promise<void> {
        if (this._client) {
            return;
        }
        if (this.subject.isStopped) {
            this.subject = new Subject<Message>();
        }

        const client = createConnection(this.driverInfo.socketPath, () =>
            this.emit(SockEvent.CONNECTED, null),
        );
        this._client = client;
        const messageProcessing = MessageProcessing.create(client, this.subject);
        client.on("data", (data) => {
            messageProcessing.processMessage(data, () => {
                this.retryAttempts = 0;
            });
        });
        client.on("close", () => {
            this.log("Socket closed.");
            this.handleSocketClose(client);
        });

        client.on("error", (err) => {
            this.emitError("Socket error: " + String(err));
            this.log("Socket error: " + String(err));
            this.retryConnection();
        });

        client.on("end", () => {
            this.log("Socket ended.");
            this.handleSocketClose(client);
        });
    }

    private handleSocketClose(client: Socket) {
        if (this._client !== client) {
            return;
        }
        this._client = null;
        if (!this.retryTimer) {
            this.emit(SockEvent.DISCONNECTED, null);
            this.subject.complete();
            UHFSockDriverV2.instance = null;
        }
    }

    public stop() {
        if (this.retryTimer) {
            clearTimeout(this.retryTimer);
            this.retryTimer = null;
        }
        if (this._client) {
            const client = this._client;
            this._client = null;
            this.emit(SockEvent.DISCONNECTED, null);
            client.end();
            client.destroy();
        }
        this.subject.complete();
        UHFSockDriverV2.instance = null;
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
        if (this.retryTimer) {
            clearTimeout(this.retryTimer);
            this.retryTimer = null;
        }
        if (this._client) {
            const client = this._client;
            this._client = null;
            client.end();
            client.destroy();
        }
        UHFSockDriverV2.instance = null;
        KillProcess.kill(this.driverInfo.pid);
        this.subject.next(new Message(SockEvent.EXIT, null));
        this.subject.complete();
    }
    public async getLogs(maxLines: number): Promise<string> {
        return GetUHFSocketLogs.start(this.driverInfo.logsPath, maxLines);
    }
    public async log(...logs: string[]): Promise<void> {
        await writeLog(this.driverInfo.logsPath, ...logs);
    }
}
