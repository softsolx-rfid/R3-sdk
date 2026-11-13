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
import { EnableCapsLock } from "./commands/enable-caps";
import { ReadMode, ReadModeParams } from "./commands/readmode";
import { GetAcuraLogs } from "./utils/get-acura-logs";
import { LogAcura } from "./utils/log-acura";

export class HexapadDriverV2 extends BaseDriver {
    public subject = new Subject<Message>();
    public name = Drivers.SERIAL_H10_V2;
    private _port: SerialPort | null = null;
    private subjectRaw = new Subject<string>();
    private messageBuffer = "";
    private sendMessagePipe: {
        fn: () => Promise<void>;
        message: string;
    }[] = [];
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
                const item = this.sendMessagePipe.shift();
                if (item) {
                    const { fn, message } = item;
                    this.cronIsRunning = true;
                    this.log("Executing command from pipe...", message);
                    const resp = await fn();
                    this.log("Command executed.", String(resp));
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
                path: usbPort,
                baudRate: 115200,
                dataBits: 8,
                stopBits: 1,
                parity: "none",
            });
            this.log(
                "HexapadDriver started.",
                "SERIAL PORT OPENED: " + `${usbPort}`,
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
            await this.sendPromise(SendSockEvent.START, null);
            await this.sendPromise(SendSockEvent.SET_CAPS_LOCK, "off");
            await this.sendPromise(SendSockEvent.READMODE, "serial");
        } catch (error) {
            this.subject.next(new Message(SockEvent.ERROR, error));
            throw error;
        }
    }

    private async stopCron() {
        if (this.pipeCron) {
            clearInterval(this.pipeCron);
            this.pipeCron = null;
            // finalize all pending messages in the pipe
            for (const item of this.sendMessagePipe) {
                const { fn, message } = item;
                this.log("Finalizing command from pipe...", message);
                try {
                    const resp = await fn();
                    this.log("Command finalized.", String(resp));
                } catch (error) {
                    this.log("Error finalizing command.", String(error));
                }
            }
            this.sendMessagePipe = [];
        }
    }

    public async stop() {
        await this.sendPromise(SendSockEvent.STOP, null);
        await this.stopCron();
        this.sendMessagePipe = [];
        this.port.close();
        this._port = null;
        this.subject.complete();
        this.subjectRaw.complete();
    }

    public send<K extends SendSockEvent>(event: K, data: SendEventMap[K]) {
        this.sendMessagePipe.push({
            fn: async () => await this.sendMessagePipeResolver(event, data),
            message: String(event),
        });
    }

    public async sendPromise<K extends SendSockEvent>(
        event: K,
        data: SendEventMap[K],
    ) {
        return new Promise((resolve, reject) => {
            this.sendMessagePipe.push({
                fn: async () => {
                    try {
                        const resp = await this.sendMessagePipeResolver(
                            event,
                            data,
                        );
                        resolve(resp);
                    } catch (error) {
                        reject(error);
                    }
                },
                message: String(event),
            });
        });
    }

    private async sendMessagePipeResolver<K extends SendSockEvent>(
        event: K,
        data: SendEventMap[K],
    ) {
        const eventHandler = this.socketEvents.find((e) => e.name === event);
        if (eventHandler) {
            await eventHandler.handler(data);
        } else {
            this.log("Unknown event: " + String(event));
            this.subject.next(
                new Message(SockEvent.ERROR, "Unknown event: " + String(event)),
            );
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
        LogAcura.log(this.logsPath, ...data);
    }

    private async createLogsFileIfNotExists() {
        const logsDir = path.dirname(this.logsPath);
        await fs.mkdir(logsDir, { recursive: true });
        const file = await fs.open(this.logsPath, "a", 0o666);
        await file.close();
    }

    public async getLogs(maxLines = 1000) {
        return await GetAcuraLogs.getLogs(this.logsPath, maxLines);
    }

    public killProcess() {
        this.subject.complete();
        this.subjectRaw.complete();
        if (this._port) {
            this._port.close();
            this._port = null;
        }
        if (this.pipeCron) {
            clearInterval(this.pipeCron);
            this.pipeCron = null;
            this.sendMessagePipe = [];
        }
        this.subject.next(
            new Message(
                SockEvent.DISCONNECTED,
                "Hexapad driver process killed",
            ),
        );
    }

    private socketEvents = [
        {
            name: SendSockEvent.STOP,
            handler: async (data: any) => {
                await ReadTag.execute(this, "off", true);
            },
        },
        {
            name: SendSockEvent.START,
            handler: async (data: any) => {
                await ReadTag.execute(this, "on", true);
            },
        },
        {
            name: SendSockEvent.DISCONNECTED,
            handler: async (data: any) => {
                this.killProcess();
            },
        },
        {
            name: SendSockEvent.EXIT,
            handler: async (data: any) => {
                this.killProcess();
            },
        },
        {
            name: SendSockEvent.GET_BEEP,
            handler: async (data: any) =>
                await EnableBeep.execute(this, "get", true),
        },
        {
            name: SendSockEvent.SET_BEEP,
            handler: async (data: any) =>
                EnableBeep.execute(this, data === true ? "on" : "off", true),
        },
        {
            name: SendSockEvent.RESET,
            handler: async (data: any) => {
                await ReadTag.execute(this, "off", true);
                await ReadTag.execute(this, "on", true);
            },
        },
        {
            name: SendSockEvent.GET_POWER,
            handler: async (data: any) => {
                await ReadPower.execute(this, 0, true);
            },
        },
        {
            name: SendSockEvent.SET_POWER,
            handler: async (data: any) => {
                await ReadPower.execute(
                    this,
                    (data as { power?: number })?.power ?? 10,
                    true,
                );
            },
        },
        {
            name: SendSockEvent.SET_CAPS_LOCK,
            handler: async (data: any) =>
                await EnableCapsLock.execute(
                    this,
                    data as "on" | "off" | "get",
                    true,
                ),
        },
        {
            name: SendSockEvent.READMODE,
            handler: async (data: any) =>
                await ReadMode.execute(this, data as ReadModeParams, true),
        },
    ];
}
