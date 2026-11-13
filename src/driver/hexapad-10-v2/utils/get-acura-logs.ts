import { UHFSocketError } from "@/errors/uhf-sock.error";
import { promises as fs } from "fs";

export class GetAcuraLogs {
    public static async getLogs(logPath: string, maxLines = 100) {
        return await new GetAcuraLogs().getLogs(logPath, maxLines);
    }
    protected constructor() {}
    public async getLogs(logPath: string, maxLines = 100) {
        try {
            const file = await fs.open(logPath, "r");
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
