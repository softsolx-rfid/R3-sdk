import { Command } from "./index";
import { HexapadDriver } from "../hexapad-driver";
import { SockEvent } from "@/index";
import { Message } from "@/messages/dto/message";
import { ReadTag } from "./read-tag";

export class EnableCapsLock extends Command<"on" | "off" | "get"> {
    constructor(driver: HexapadDriver, params: "on" | "off" | "get") {
        super(driver, params);
    }
    command = "enablecapslock";

    static execute(
        driver: HexapadDriver,
        params: "on" | "off" | "get",
        silent = false,
    ): Promise<string> {
        const command = new EnableCapsLock(driver, params);
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
                        ? SockEvent.GET_CAPS_LOCK
                        : SockEvent.SET_CAPS_LOCK,
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
