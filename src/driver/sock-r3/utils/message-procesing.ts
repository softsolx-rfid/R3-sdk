import { SockEvent } from "@/driver/events.enum";
import { UHFSocketError } from "@/errors/uhf-sock.error";
import { Message } from "@/messages/dto/message";
import { Socket } from "net";
import { Subject } from "rxjs";

export class MessageProcessing {
    public static create(client: Socket, subject: Subject<Message<any>>) {
        return new MessageProcessing(client, subject);
    }
    private dataBuffer = "";
    constructor(
        private readonly client: Socket,
        private readonly subject: Subject<Message<any>>,
    ) {}

    public async processMessage(
        data: string | Buffer<ArrayBuffer>,
        onPing: () => void,
    ) {
        if (data.toString() === "PING") {
            onPing();
            this.client.write("PONG\n");
            return;
        }
        try {
            this.dataBuffer += data.toString();
            let line = this.dataBuffer.indexOf("\n");
            while (line !== -1) {
                const messageStr = this.dataBuffer.slice(0, line);
                this.dataBuffer = this.dataBuffer.slice(line + 1);
                try {
                    const message = JSON.parse(messageStr);
                    this.emit(message.event, message.data);
                } catch (error) {
                    this.emitError("Error parsing message: " + String(error));
                }
                line = this.dataBuffer.indexOf("\n");
            }
        } catch (error) {
            this.emitError("Error processing data: " + String(error));
        }
    }

    private emit<T>(event: SockEvent, data: T) {
        this.subject.next(new Message(event, data));
    }

    private emitError(message: string) {
        this.subject.next(
            new Message(SockEvent.ERROR, new UHFSocketError(message)),
        );
    }
}
