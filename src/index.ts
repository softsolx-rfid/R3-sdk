import { Subscription } from "rxjs";
import { Message } from "./messages/dto/message";
import { SockEvent } from "./sock/events.enum";
import { UhfSockClient } from "./sock/uhf-sock.client";
import { EventMap } from "./@types/event-map";
import { SendEventMap } from "./@types/send-event-map";
import { SendSockEvent } from "./sock/send-events.enum";
import { Antenna } from "./@types/antenna.enum";
import { UHFSocketSetup } from "./dto/uhf-socket-setup";
import { UHFSocketError } from "./errors/uhf-sock.error";

class UhfSocket {
  private connexion = new UhfSockClient();
  private static subscriptions: Subscription[] = [];
  private static instance: UhfSocket;
  private static started: boolean = false;

  constructor(private setup: UHFSocketSetup) {
    if (UhfSocket.instance) {
      return UhfSocket.instance;
    }
    UhfSocket.instance = this;
  }

  public inicialize() {
    if (UhfSocket.started) {
      return;
    }
    UhfSocket.started = true;
    this.connexion.start();
    this.send(SendSockEvent.RESET, null);
    this.send(SendSockEvent.SET_BEEP, this.setup.beep);
    this.send(SendSockEvent.SET_POWER, {
      power: this.setup.power,
      antenna: Antenna.ALL
    });
  }

  public stop() {
    this.connexion.stop();
    UhfSocket.subscriptions.forEach(subscription => subscription.unsubscribe());
    UhfSocket.subscriptions = [];
    UhfSocket.started = false;
  }

  public get observable() {
    return this.connexion.observable;
  }

  public send<K extends SendSockEvent>(event: K, data: SendEventMap[K]) {
    const message = new Message(event as unknown as SockEvent, data);
    this.connexion.sendMessage(message);
  }

  public on<K extends SockEvent>(event: K, callback: EventMap[K]) {
    const subscription: Subscription = this.connexion.observable.subscribe((message) => {
      if (message.event === event) {
        callback(message as any);
      }
    });
    UhfSocket.subscriptions.push(subscription);
    return subscription;
  }

  public onAll(callback: (message: Message) => void) {
    const subscription: Subscription = this.connexion.observable.subscribe((message) => {
      callback(message);
    });
    UhfSocket.subscriptions.push(subscription);
    return subscription;
  }

  public killProcess() {
    this.connexion.killProcess();
    this.stop();
  }
}

export default UhfSocket;
export { SockEvent, SendSockEvent, Antenna, UHFSocketError };