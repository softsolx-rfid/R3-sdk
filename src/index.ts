import { Subscription } from "rxjs";
import { Message } from "./messages/dto/message";
import { EventMap } from "./@types/event-map";
import { SendEventMap } from "./@types/send-event-map";
import { Antenna } from "./@types/antenna.enum";
import { UHFSocketError } from "./errors/uhf-sock.error";
import { BaseDriver } from "./driver/base-driver.abstract";
import { SendSockEvent } from "./driver/send-events.enum";
import { SockEvent } from "./driver/events.enum";
import { UhfSockDriver } from "./driver/sock-r3/uhf-sock.driver";
import { HexapadDriver } from "./driver/hexapad-10/hexapad-driver";

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
            case Drivers.SERIAL_H10:
                this._connection = new HexapadDriver();
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

    public async inicialice() {
        try {
            if (this.connection.isRunning) {
                throw new UHFSocketError(
                    "UHF Socket is already started. Please stop it before initializing again.",
                );
            }
            await this.connection.start();
            this.send(SendSockEvent.RESET, null);
            this.on(SockEvent.DISCONNECTED, () => {
                UhfSocket.subscriptions.forEach((subscription) =>
                    subscription.unsubscribe(),
                );
                UhfSocket.subscriptions = [];
            });
        } catch (error) {}
    }

    public async stop() {
        if (!this.connection.isRunning) {
            throw new UHFSocketError(
                "UHF Socket is not started. Please start it before stopping.",
            );
        }
        await this.connection.stop();
        UhfSocket.subscriptions.forEach((subscription) =>
            subscription.unsubscribe(),
        );
        UhfSocket.subscriptions = [];
    }

    public send<K extends SendSockEvent>(event: K, data: SendEventMap[K]) {
        this.connection.send(event, data);
    }

    public async sendRaw(data: string): Promise<void> {
        await this.connection.sendRaw(data);
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
