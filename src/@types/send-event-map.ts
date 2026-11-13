import { SendSockEvent } from "@/driver/send-events.enum";
import { Antenna } from "./antenna.enum";
import { ReadModeParams } from "@/driver/hexapad-10/commands/readmode";

export type SendEventMap = {
    [SendSockEvent.DISCONNECTED]: null;
    [SendSockEvent.START]: null;
    [SendSockEvent.STOP]: null;
    [SendSockEvent.RESET]: null;
    [SendSockEvent.SET_POWER]: { power: number; antenna: Antenna };
    [SendSockEvent.GET_POWER]: null;
    [SendSockEvent.SET_BEEP]: boolean;
    [SendSockEvent.GET_BEEP]: null;
    [SendSockEvent.EXIT]: null;
    [SendSockEvent.SET_CAPS_LOCK]: "on" | "off" | "get";
    [SendSockEvent.READMODE]: ReadModeParams;
};
