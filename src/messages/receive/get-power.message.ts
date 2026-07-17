import { SockEvent } from "@/sock/events.enum";
import { Message } from "@/messages/dto/message";

export class GetPowerMessage extends Message<Array<number>> {
  constructor(s: Array<number>) {
    super(SockEvent.GET_POWER, s);
  }

  public static create(s: Array<number>): GetPowerMessage {
    return new GetPowerMessage(s);
  }
}