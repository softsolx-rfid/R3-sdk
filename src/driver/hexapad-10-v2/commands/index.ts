import { UHFSocketError } from "@/errors/uhf-sock.error";
import { HexapadDriver } from "../hexapad-driver";

export abstract class Command<T> {
    constructor(
        protected readonly driver: HexapadDriver,
        protected readonly params: T,
    ) {}
    abstract command: string;
    abstract execute(): Promise<string>;

    protected sendCommand(overrideCommand?: string) {
        return new Promise<string>((resolve, reject) => {
            this.driver.log(
                `Executing command: ${overrideCommand ?? this.command} ${this.params}`,
            );
            const to = setTimeout(() => {
                reject(new UHFSocketError("Timeout waiting for response"));
                this.driver.log(
                    `Command ${this.command} ${this.params} timed out.`,
                );
                sub.unsubscribe();
            }, 5000); // 5 seconds timeout
            this.driver.sendRaw(
                overrideCommand ?? `${this.command} ${this.params}`,
            );
            const sub = this.driver.onAllRaw((data) => {
                clearTimeout(to);
                this.driver.log(`Received response: ${data}`);
                sub.unsubscribe();
                resolve(data);
            });
        });
    }
}
