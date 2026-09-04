import { Command } from "./index";
import { HexapadDriver } from "../hexapad-driver";
import { UHFSocketError, SockEvent } from "@/index";
import { Message } from "@/messages/dto/message";
import { ReadTag } from "./read-tag";

export class ReadPower extends Command<number> {
    constructor(driver: HexapadDriver, params: number) {
        super(driver, params);
    }
    command = "readpower";

    static execute(
        driver: HexapadDriver,
        params: number,
        silent = false,
    ): Promise<string> {
        const command = new ReadPower(driver, params);
        return command.execute(silent);
    }

    async execute(silent = false): Promise<string> {
        try {
            await ReadTag.execute(this.driver, "off");
            const resp = await this.sendCommand(
                this.params === 0
                    ? this.command
                    : `${this.command} ${this.params}`,
            );
            const power = Number(resp.replace(/\D/g, ""));
            this.driver.subject.next(
                new Message(
                    this.params === 0
                        ? SockEvent.GET_POWER
                        : SockEvent.SET_POWER,
                    [power, power, power, power],// to no broken the default interface for 4 antenna,
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
