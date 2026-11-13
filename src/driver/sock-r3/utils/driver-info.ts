import { UHFSocketError } from "@/errors/uhf-sock.error";
import { readFileSync } from "fs";

export class R3DriverInfo {
    public socketPath: string;
    public logsPath: string;
    public pid: number;

    constructor() {
        try {
            const driverInfo = JSON.parse(
                readFileSync("/var/uhf/uhf.var", "utf8"),
            );
            if (
                !driverInfo.socketPath ||
                !driverInfo.logsPath ||
                !driverInfo.pid
            ) {
                throw new UHFSocketError(
                    "Invalid UHF socket variable file. Please ensure that the UHF socket server is running and the /var/uhf/uhf.var file contains valid information.",
                );
            }
            this.socketPath = driverInfo.socketPath;
            this.logsPath = driverInfo.logsPath;
            this.pid = driverInfo.pid;
        } catch (error) {
            throw new UHFSocketError(
                "UHF socket variable file not found. Please ensure that the UHF socket server is running and the /var/uhf/uhf.var file exists.",
            );
        }
    }
}
