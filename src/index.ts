import { Subscription } from "rxjs";
import { Message } from "./messages/dto/message";
import { EventMap } from "./@types/event-map";
import { SendEventMap } from "./@types/send-event-map";
import { Antenna } from "./@types/antenna.enum";
import { UHFSocketSetup } from "./dto/uhf-socket-setup";
import { UHFSocketError } from "./errors/uhf-sock.error";
import { BaseDriver } from "./driver/base-driver.abstract";
import { SendSockEvent } from "./driver/send-events.enum";
import { SockEvent } from "./driver/events.enum";
import { UhfSockDriver } from "./driver/sock-r3/uhf-sock.driver";

export enum Drivers {
    UHF_SOCKET_R3 = "uhf-socket-r3",
    SERIAL_H10 = "serial-h10",
}

class UhfSocket {
    private _connection: BaseDriver | null = null;
    private static subscriptions: Subscription[] = [];
    private static instance: UhfSocket;

    constructor(driver: Drivers) {
        if (UhfSocket.instance) {
            return UhfSocket.instance;
        }
        switch (driver) {
            case Drivers.UHF_SOCKET_R3:
                this._connection = new UhfSockDriver();
                break;
            default:
                throw new UHFSocketError("Unsupported driver");
        }
        UhfSocket.instance = this;
    }

    private get connection(): BaseDriver {
        if (!this._connection) {
            throw new UHFSocketError(
                "Connection is not initialized. Call inicialice() first.",
            );
        }
        return this._connection;
    }

    public get isStarted() {
        return this.connection.isRunning;
    }

    public inicialice() {
        if (this.connection.isRunning) {
            throw new UHFSocketError(
                "UHF Socket is already started. Please stop it before initializing again.",
            );
        }
        this.connection.start();
        this.send(SendSockEvent.RESET, null);
        this.on(SockEvent.DISCONNECTED, () => {
            UhfSocket.subscriptions.forEach((subscription) =>
                subscription.unsubscribe(),
            );
            UhfSocket.subscriptions = [];
        });
    }

    public stop() {
        if (!this.connection.isRunning) {
            throw new UHFSocketError(
                "UHF Socket is not started. Please start it before stopping.",
            );
        }
        this.connection.stop();
        UhfSocket.subscriptions.forEach((subscription) =>
            subscription.unsubscribe(),
        );
        UhfSocket.subscriptions = [];
    }

    public send<K extends SendSockEvent>(event: K, data: SendEventMap[K]) {
        this.connection.send(event, data);
    }

    public on<K extends SockEvent>(
        event: K,
        callback: EventMap[K],
    ): Subscription {
        const sub = this.connection.on(event, callback);
        UhfSocket.subscriptions.push(sub);
        return sub;
    }

    public onAll(callback: (message: Message) => void) {
        const sub = this.connection.onAll(callback);
        UhfSocket.subscriptions.push(sub);
        return sub;
    }

    public killProcess() {
        this.connection.killProcess();
        this.stop();
    }

    public async getLogs(maxLines = 1000) {
        return await this.connection.getLogs(maxLines);
    }
}

export default UhfSocket;
export { SockEvent, SendSockEvent, Antenna, UHFSocketError };
