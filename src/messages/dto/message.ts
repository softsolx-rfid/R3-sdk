import { SockEvent } from "../../sock/events.enum";

export class Message<T = any> {
  public event: SockEvent;
  public data: T;

  constructor(event: SockEvent, data: T) {
    this.event = event;
    this.data = data;
  }

  public toJson(): string {
    return JSON.stringify({
      event: this.event,
      data: this.data
    });
  }
}