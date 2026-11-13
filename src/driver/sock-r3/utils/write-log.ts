import { UHFSocketError } from "@/index";

export async function writeLog(
    logsPath: string,
    ...logs: string[]
): Promise<void> {
    const fs = await import("fs/promises");
    try {
        const file = await fs.open(logsPath, "a");
        await file.write(
            logs.map((s) => `${new Date().toISOString()} - ${s} \n`).join(""),
        );
        await file.close();
    } catch (error) {
        throw new UHFSocketError("Error appending to logs: " + String(error));
    }
}
