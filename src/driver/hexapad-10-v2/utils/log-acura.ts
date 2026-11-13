import { promises as fs } from "fs";
import { UHFSocketError } from "@/errors/uhf-sock.error";

export class LogAcura {
    public static async log(logsPath: string, ...data: string[]) {
        try {
            const file = await fs.open(logsPath, "a");

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
}
