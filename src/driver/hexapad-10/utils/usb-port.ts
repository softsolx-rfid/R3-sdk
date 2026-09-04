import { UHFSocketError } from "@/errors/uhf-sock.error";
import { execPromise } from "@/utils/exec-promise";

export class USBPort {
    static async getUSBPorts(): Promise<string> {
        const usbPortInstance = new USBPort();
        return usbPortInstance.findUSBPorts();
    }

    public async findUnix() {
        try {
            const usbPortList = await execPromise("ls -a /dev/ | grep ttyACM0");
            const usbPort = usbPortList.split("\n")[0];
            if (!usbPort || usbPort.trim() === "") {
                return null;
            }
            return usbPort;
        } catch (error) {
            return null;
        }
    }

    public async findMac() {
        try {
            const usbPortList = await execPromise(
                "ls -a /dev/ | grep cu.usbmodem",
            );
            const usbPort = usbPortList.split("\n")[0];
            if (!usbPort || usbPort.trim() === "") {
                return null;
            }
            return usbPort;
        } catch (error) {
            return null;
        }
    }

    private async findUSBPorts(): Promise<string> {
        const unix = await this.findUnix();
        if (unix) {
            return unix;
        }
        const mac = await this.findMac();
        if (mac) {
            return mac;
        }
        throw new UHFSocketError("No USB ports found");
    }
}
