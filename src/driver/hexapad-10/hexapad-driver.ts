import { Drivers, UHFSocketError } from "@/index";
import { BaseDriver } from "../base-driver.abstract";
import { SendEventMap } from "@/@types/send-event-map";
import { SendSockEvent } from "../send-events.enum";
import { SockEvent } from "../events.enum";
import { Subject, Subscription } from "rxjs";
import { EventMap } from "@/@types/event-map";
import { Message } from "@/messages/dto/message";
import { SerialPort } from "serialport";
import { promises as fs } from "fs";
import path from "path";
import { ReadTag } from "./commands/read-tag";
import { USBPort } from "./utils/usb-port";
import { EnableBeep } from "./commands/enable-beep";
import { ReadPower } from "./commands/read-power";

export class HexapadDriver extends BaseDriver {
    public subject = new Subject<Message>();
    public name = Drivers.SERIAL_H10;
    private _port: SerialPort | null = null;
    private subjectRaw = new Subject<string>();
    private messageBuffer = "";
    private sendMessagePipe: (() => void)[] = [];
    private pipeCron: NodeJS.Timeout | null = null;
    private cronIsRunning = false;

    constructor() {
        super();
    }

    get isRunning() {
        return !!this._port;
    }

    private get port() {
        if (!this._port) {
            throw new UHFSocketError("Serial port is not initialized.");
        }
        return this._port;
    }

    private startCron() {
        if (this.pipeCron) {
            clearInterval(this.pipeCron);
        }
        this.pipeCron = setInterval(async () => {
            if (this.cronIsRunning) {
                return;
            }
            try {
                const fn = this.sendMessagePipe.shift();
                if (fn) {
                    this.cronIsRunning = true;
                    console.log("Executing command from pipe...");
                    await fn();
                    console.log("Command executed.");
                    this.cronIsRunning = false;
                }
            } catch (error) {
                this.cronIsRunning = false;
            }
        }, 100);
    }

    public async start() {
        try {
            this.log("Starting HexapadDriver...");
            this.subject.complete();
            this.subjectRaw.complete();
            this.subject = new Subject<Message>();
            this.subjectRaw = new Subject<string>();
            await this.createLogsFileIfNotExists();
            const usbPort = await USBPort.getUSBPorts();
            this._port = new SerialPort({
                path: `/dev/${usbPort}`,
                baudRate: 115200,
                dataBits: 8,
                stopBits: 1,
                parity: "none",
            });
            this.log(
                "HexapadDriver started.",
                "SERIAL PORT OPENED: " + `/dev/${usbPort}`,
            );
            this.port.on("data", (data) => {
                const chunk: string = data.toString();
                this.messageBuffer += chunk;
                if (chunk.includes("\r\n")) {
                    const message = this.messageBuffer.replace(">", "");
                    this.messageBuffer = "";
                    this.subjectRaw.next(message);
                    if (!this.cronIsRunning) {
                        this.subject.next(
                            new Message(
                                SockEvent.TAG,
                                message.replace("\r\n", "").toUpperCase(),
                            ),
                        );
                    }
                }
            });
            this.port.on("error", (err) => {
                this.log("Serial port error: " + String(err));
            });
            this.startCron();
            this.log(await ReadTag.execute(this, "on"));
        } catch (error) {
            console.log(error);
            this.subject.next(new Message(SockEvent.ERROR, error));
        }
    }

    private stopCron() {
        if (this.pipeCron) {
            clearInterval(this.pipeCron);
            this.pipeCron = null;
        }
    }

    public async stop() {
        const resp = await ReadTag.execute(this, "off");
        this.stopCron();
        this.sendMessagePipe = [];
        this.log(resp);
    }

    public send<K extends SendSockEvent>(event: K, data: SendEventMap[K]) {
        this.sendMessagePipe.push(() =>
            this.sendMessagePipeResolver(event, data),
        );
    }

    private async sendMessagePipeResolver<K extends SendSockEvent>(
        event: K,
        data: SendEventMap[K],
    ) {
        switch (event) {
            case SendSockEvent.STOP:
                await ReadTag.execute(this, "off", true);
                break;
            case SendSockEvent.START:
                await ReadTag.execute(this, "on", true);
                break;
            case SendSockEvent.DISCONNECTED:
                this.killProcess();
                break;
            case SendSockEvent.EXIT:
                this.killProcess();
                break;
            case SendSockEvent.GET_BEEP:
                await EnableBeep.execute(this, "get", true);
                break;
            case SendSockEvent.SET_BEEP:
                await EnableBeep.execute(
                    this,
                    data === true ? "on" : "off",
                    true,
                );
                break;
            case SendSockEvent.RESET:
                await ReadTag.execute(this, "off", true);
                await ReadTag.execute(this, "on", true);
                break;
            case SendSockEvent.GET_POWER:
                await ReadPower.execute(this, 0, true);
                break;
            case SendSockEvent.SET_POWER:
                await ReadPower.execute(
                    this,
                    (data as { power?: number })?.power ?? 10,
                    true,
                );
                break;
            default:
                this.log("Unknown event: " + String(event));
                this.subject.next(
                    new Message(
                        SockEvent.ERROR,
                        "Unknown event: " + String(event),
                    ),
                );
                break;
        }
    }

    public on<K extends SockEvent>(
        event: K,
        callback: EventMap[K],
    ): Subscription {
        return this.subject.subscribe((message) => {
            if (message.event === event) {
                callback(message as unknown as any);
            }
        });
    }

    public onAll(callback: (message: Message) => void): Subscription {
        return this.subject.subscribe((message) => {
            callback(message);
        });
    }

    public killProcess() {
        if (this._port) {
            this._port.close();
            this._port = null;
        }
        this.subject.next(
            new Message(
                SockEvent.DISCONNECTED,
                "Hexapad driver process killed",
            ),
        );
    }

    // Raw data handling methods

    /**
     * @param data The raw data string to send to the device.
     * @description Sends a raw data string directly to the device.
     */

    public async sendRaw(data: string): Promise<void> {
        this.port.write(`\n${data}\r\n`);
    }

    /**
     * @param callback The callback function to handle raw messages.
     * @returns A subscription to the raw message stream.
     * @description Use only if you need to handle raw messages directly from the device.
     */

    public onAllRaw(callback: (message: string) => void): Subscription {
        return this.subjectRaw.subscribe((message) => {
            callback(message);
        });
    }
    // logs-related methods are implemented below
    private readonly logsPath = path.join(process.cwd(), "logs", "hexapad.log");

    public async log(...data: string[]) {
        try {
            const file = await fs.open(this.logsPath, "a");

            await file.write(
                data
                    .map((s) => `${new Date().toISOString()} - ${s} \n`)
                    .join(""),
            );
            await file.close();
        } catch (error) {
            throw new UHFSocketError(
                "Error appending to logs: " + String(error),
            );
        }
    }

    private async createLogsFileIfNotExists() {
        const logsDir = path.dirname(this.logsPath);

        await fs.mkdir(logsDir, { recursive: true });

        const file = await fs.open(this.logsPath, "a", 0o666);
        await file.close();
    }

    public async getLogs(maxLines = 1000) {
        try {
            const file = await fs.open(this.logsPath, "r");
            const stats = await file.stat();
            const fileSize = stats.size;
            const bufferSize = Math.min(fileSize, 5 * 1024 * 1024); // Read up to 5MB
            const buffer = Buffer.alloc(bufferSize);
            await file.read(buffer, 0, bufferSize, fileSize - bufferSize);
            await file.close();

            const lines = buffer.toString().split("\n");
            return lines.slice(-maxLines).join("\n");
        } catch (error) {
            throw new UHFSocketError("Error reading logs: " + String(error));
        }
    }
}
