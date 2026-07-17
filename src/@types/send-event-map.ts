import { SendSockEvent } from "@/sock/send-events.enum";
import { Antenna } from "./antenna.enum";


export type SendEventMap = {
  [SendSockEvent.DISCONNECTED]: null;
  [SendSockEvent.START]: null;
  [SendSockEvent.STOP]: null;
  [SendSockEvent.RESET]: null;
  [SendSockEvent.SET_POWER]: { power: number, antenna: Antenna };
  [SendSockEvent.GET_POWER]: null;
  [SendSockEvent.SET_BEEP]: boolean;
  [SendSockEvent.GET_BEEP]: null;
  [SendSockEvent.EXIT]: null;
};