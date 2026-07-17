import net from 'net';
import { Message } from '../messages/dto/message';
import { Subject } from 'rxjs';
import { SockEvent } from './events.enum';
import { readFileSync } from 'fs';
import { UHFSocketError } from '@/errors/uhf-sock.error';
import * as fs from 'fs/promises';

export class UhfSockClient {
  private subject = new Subject<Message>();
  private _client: net.Socket | null = null;
  private static instance: UhfSockClient;
  private driverInfo: {
    socketPath: string,
    logsPath: string,
    pid: number
  } | null = null;

  constructor() {
    if (UhfSockClient.instance) {
      return UhfSockClient.instance;
    }
    this.setup();
    UhfSockClient.instance = this;
  }

  private setup() {
    try {
      this.driverInfo = JSON.parse(readFileSync('/var/uhf/uhf.var', 'utf8'));
    } catch (error) {
      throw new UHFSocketError('UHF socket variable file not found. Please ensure that the UHF socket server is running and the /var/uhf/uhf.var file exists.');
    }
  }

  private get client(): net.Socket {
    if (!this._client) {
      throw new UHFSocketError('Client is not initialized. Call start() first.');
    }
    return this._client;
  }

  public get observable() {
    return this.subject.asObservable();
  }

  public start() {
    if (!this.driverInfo) {
      throw new UHFSocketError('Driver info not available. Ensure that the UHF socket server is running and the /var/uhf/uhf.var file exists.');
    }
    this._client = net.createConnection(this.driverInfo.socketPath, () => {
      this.subject.next(new Message(SockEvent.CONNECTED, null));
    })

    this.client.on('data', (data) => {
      if (data.toString() === "PING") {
        this.client.write("PONG\n");
        return;
      }
      try {
        const messaje = JSON.parse(data.toString());
        this.subject.next(new Message(messaje.event, messaje.data));
      } catch (error) {
        if (error instanceof SyntaxError) {
          return;
        }
        console.error('Error parsing JSON:', error);
      }
    });

    this.client.on('end', () => {
      this.subject.next(new Message(SockEvent.DISCONNECTED, null));
    });
  }

  public stop() {
    if (this._client) {
      this.client.end();
      this._client = null;
      this.subject.next(new Message(SockEvent.DISCONNECTED, null));
    }
  }

  public killProcess() {
    if (this.driverInfo?.pid) {
      try {
        process.kill(this.driverInfo.pid);
        this.subject.next(new Message(SockEvent.EXIT, null));
      } catch (error) {
        console.error('Error killing process:', error);
      }
    } else { throw new UHFSocketError('Driver PID not available. Ensure that the UHF socket server is running and the /var/uhf/uhf.var file exists.'); }
  }

  public sendMessage<T>(message: Message<T>) {
    this.client.write(message.toJson() + '\n');
  }

  public async getLogs(maxLines = 1000) {
    if (!this.driverInfo) {
      throw new UHFSocketError('Driver info not available. Ensure that the UHF socket server is running and the /var/uhf/uhf.var file exists.');
    }
    try {
      const file = await fs.open(this.driverInfo.logsPath, 'r');
      const stats = await file.stat();
      const fileSize = stats.size;
      const bufferSize = Math.min(fileSize, 5 * 1024 * 1024); // Read up to 5MB
      const buffer = Buffer.alloc(bufferSize);
      await file.read(buffer, 0, bufferSize, fileSize - bufferSize);
      await file.close();

      const lines = buffer.toString().split('\n');
      return lines.slice(-maxLines).join('\n');
    } catch (error) {
      throw new UHFSocketError('Error reading logs: ' + String(error));
    }
  }
}