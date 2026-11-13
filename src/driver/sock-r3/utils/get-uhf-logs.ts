import { open } from "fs/promises";

export class GetUHFSocketLogs {
    constructor(private logsPath: string) {}
    public static async start(logsPath: string, maxLines: number) {
        const instance = new GetUHFSocketLogs(logsPath);
        return instance.getLogsData(maxLines);
    }
    private async getLogsData(maxLines: number) {
        const file = await open(this.logsPath, "r");
        const stats = await file.stat();
        const fileSize = stats.size;
        const bufferSize = Math.min(fileSize, 5 * 1024 * 1024); // Read up to 5MB
        const buffer = Buffer.alloc(bufferSize);
        await file.read(buffer, 0, bufferSize, fileSize - bufferSize);
        await file.close();

        const lines = buffer.toString().split("\n");
        return lines.slice(-maxLines).join("\n");
    }
}
