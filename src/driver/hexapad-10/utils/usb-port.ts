import { execPromise } from "@/utils/exec-promise";

export class USBPort {
    static async getUSBPorts(): Promise<string> {
        const usbPortInstance = new USBPort();
        return usbPortInstance.findUSBPorts();
    }

    private async findUSBPorts(): Promise<string> {
        const usbPortList = await execPromise("ls -a /dev/ | grep cu.usbmodem");
        const usbPort = usbPortList.split("\n")[0];
        return usbPort;
    }
}
