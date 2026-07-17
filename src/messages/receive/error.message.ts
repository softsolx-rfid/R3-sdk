import { SockEvent } from "@/sock/events.enum";
import { Message } from "@/messages/dto/message";

export class ErrorMessage extends Message<string> {
  constructor(data: string) {
    super(SockEvent.ERROR, data);
  }

  public static create(data: string): ErrorMessage {
    return new ErrorMessage(data);
  }
}