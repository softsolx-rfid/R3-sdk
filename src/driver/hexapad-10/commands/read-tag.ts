import { Command } from "./index";
import { HexapadDriver } from "../hexapad-driver";
import { SockEvent } from "@/index";
import { Message } from "@/messages/dto/message";

export class ReadTag extends Command<"on" | "off"> {
    constructor(driver: HexapadDriver, params: "on" | "off") {
        super(driver, params);
    }
    command = "readtag";

    static execute(
        driver: HexapadDriver,
        params: "on" | "off",
        silent = false,
    ): Promise<string> {
        const command = new ReadTag(driver, params);
        return command.execute(silent);
    }

    async execute(silent = false): Promise<string> {
        try {
            const resp = await this.sendCommand();
            this.driver.subject.next(
                new Message(
                    this.params === "on" ? SockEvent.START : SockEvent.STOP,
                    resp,
                ),
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
        }
    }
}
