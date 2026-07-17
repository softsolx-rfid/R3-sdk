import { Message } from "@/messages/dto/message";
import { ErrorMessage } from "@/messages/receive/error.message";
import { GetPowerMessage } from "@/messages/receive/get-power.message";
import { SockEvent } from "@/sock/events.enum";

export type EventMap = {
  [SockEvent.CONNECTED]: (message: Message<string>) => void;
  [SockEvent.DISCONNECTED]: (message: Message<string>) => void;
  [SockEvent.TAG]: (message: Message<string>) => void;
  [SockEvent.ERROR]: (message: ErrorMessage) => void;
  [SockEvent.START]: (message: Message<string>) => void;
  [SockEvent.STOP]: (message: Message<string>) => void;
  [SockEvent.RESET]: (message: Message<string>) => void;
  [SockEvent.SET_POWER]: (message: Message<string>) => void;
  [SockEvent.GET_POWER]: (message: GetPowerMessage) => void;
  [SockEvent.SET_BEEP]: (message: Message<string>) => void;
  [SockEvent.GET_BEEP]: (message: Message<boolean>) => void;
  [SockEvent.EXIT]: (message: Message<null>) => void;
};