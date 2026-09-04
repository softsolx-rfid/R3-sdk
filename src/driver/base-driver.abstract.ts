import { SendEventMap } from "@/@types/send-event-map";
import { SendSockEvent } from "./send-events.enum";
import { SockEvent } from "./events.enum";
import { Subscription } from "rxjs";
import { EventMap } from "@/@types/event-map";
import { Message } from "@/messages/dto/message";
import { Drivers } from "..";

export abstract class BaseDriver {
    public abstract readonly name: Drivers;
    public abstract isRunning: boolean;

    /**
     * @param maxLines number
     * @returns Promise<string[]>
     * @description Retrieves the logs from the driver, limited to the specified number of lines.
     */
    public abstract getLogs(maxLines: number): Promise<string>;

    /**
     * @description Kills the driver process.
     */
    public abstract killProcess(): void;

    /**
     * @description Starts the driver process.
     */
    public abstract start(): void;

    /**
     * @description Stops the driver process.
     */
    public abstract stop(): void;

    /**
     * @description Sends predefined message to the driver.
     * @param event The event to send to the driver.
     * @param data The data associated with the event.
     */
    public abstract send<K extends SendSockEvent>(
        event: K,
        data: SendEventMap[K],
    ): void;

    /**
     * @description Subscribes to a specific event from the driver.
     * @param event The event to subscribe to.
     * @param callback The callback to invoke when the event occurs.
     * @returns Subscription object to manage the event subscription.
     */

    public abstract on<K extends SockEvent>(
        event: K,
        callback: EventMap[K],
    ): Subscription;

    /**
     * @description Subscribes to all events from the driver.
     * @param callback The callback to invoke when any event occurs.
     * @returns Subscription object to manage the event subscription.
     */

    public abstract onAll(callback: (message: Message) => void): Subscription;
}
