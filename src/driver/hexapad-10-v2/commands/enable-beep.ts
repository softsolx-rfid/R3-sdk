import { Command } from "./index";
import { HexapadDriver } from "../hexapad-driver";
import { UHFSocketError, SockEvent } from "@/index";
import { Message } from "@/messages/dto/message";
import { ReadTag } from "./read-tag";

export class EnableBeep extends Command<"on" | "off" | "get"> {
    constructor(driver: HexapadDriver, params: "on" | "off" | "get") {
        super(driver, params);
    }
    command = "enablebeep";

    static execute(
        driver: HexapadDriver,
        params: "on" | "off" | "get",
        silent = false,
    ): Promise<string> {
        const command = new EnableBeep(driver, params);
        return command.execute(silent);
    }

    async execute(silent = false): Promise<string> {
        try {
            await ReadTag.execute(this.driver, "off");
            const resp = await this.sendCommand(
                this.params === "get"
                    ? this.command
                    : `${this.command} ${this.params}`,
            );
            this.driver.subject.next(
                new Message(
                    this.params === "get"
                        ? SockEvent.GET_BEEP
                        : SockEvent.SET_BEEP,
                    resp.includes("on"),
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
        } finally {
            await ReadTag.execute(this.driver, "on");
        }
    }
}
