import { Command } from "./index";
import { HexapadDriver } from "../hexapad-driver";
import { SockEvent } from "@/index";
import { Message } from "@/messages/dto/message";
import { ReadTag } from "./read-tag";

export type ReadModeParams = "hid" | "hidserial" | "serial" | "transparent";

export class ReadMode extends Command<ReadModeParams> {
    constructor(driver: HexapadDriver, params: ReadModeParams) {
        super(driver, params);
    }
    command = "readmode";

    static execute(
        driver: HexapadDriver,
        params: ReadModeParams,
        silent = false,
    ): Promise<string> {
        const command = new ReadMode(driver, params);
        return command.execute(silent);
    }

    async execute(silent = false): Promise<string> {
        try {
            await ReadTag.execute(this.driver, "off");
            const resp = await this.sendCommand(
                `${this.command} ${this.params}`,
            );
            this.driver.subject.next(
                new Message(SockEvent.READMODE, resp.includes("on")),
            );
            return resp;
        } catch (error) {
            this.driver.subject.next(
                new Message(
                    SockEvent.ERROR,
                    error instanceof Error ? error.message : String(error),
                ),
            );
            if (silent) {
                return "SILENT ERROR";
            }
            throw error;
        } finally {
            await ReadTag.execute(this.driver, "on");
        }
    }
}
