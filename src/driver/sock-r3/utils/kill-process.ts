import { UHFSocketError } from "@/errors/uhf-sock.error";

export class KillProcess {
    public static kill(pid: number) {
        try {
            if (process.getuid && process.getuid() !== 0) {
                throw new UHFSocketError(
                    "Insufficient privileges to kill the process. Please run the application with sudo or as root.",
                );
            }
            process.kill(pid);
        } catch (error) {
            console.error("Error killing process:", error);
        }
    }
}
