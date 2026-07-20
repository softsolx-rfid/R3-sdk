// src/messages/dto/message.ts
var Message = class {
  constructor(event, data) {
    this.event = event;
    this.data = data;
  }
  toJson() {
    return JSON.stringify({
      event: this.event,
      data: this.data
    });
  }
};

// src/sock/events.enum.ts
var SockEvent = /* @__PURE__ */ ((SockEvent2) => {
  SockEvent2["ERROR"] = "ERROR";
  SockEvent2["CONNECTED"] = "CONNECTED";
  SockEvent2["DISCONNECTED"] = "DISCONNECTED";
  SockEvent2["START"] = "START";
  SockEvent2["STOP"] = "STOP";
  SockEvent2["RESET"] = "RESET";
  SockEvent2["SET_POWER"] = "SET_POWER";
  SockEvent2["GET_POWER"] = "GET_POWER";
  SockEvent2["SET_BEEP"] = "SET_BEEP";
  SockEvent2["GET_BEEP"] = "GET_BEEP";
  SockEvent2["TAG"] = "TAG";
  SockEvent2["TAG_RAW"] = "TAG_RAW";
  SockEvent2["EXIT"] = "EXIT";
  return SockEvent2;
})(SockEvent || {});

// src/sock/uhf-sock.client.ts
import net from "net";
import { Subject } from "rxjs";
import { readFileSync } from "fs";

// src/errors/uhf-sock.error.ts
var UHFSocketError = class extends Error {
  constructor(message) {
    super(message);
    this.name = "UHFSocketError";
  }
};

// src/sock/uhf-sock.client.ts
import * as fs from "fs/promises";
var UhfSockClient = class _UhfSockClient {
  constructor() {
    this.subject = new Subject();
    this._client = null;
    this.driverInfo = null;
    if (_UhfSockClient.instance) {
      return _UhfSockClient.instance;
    }
    this.setup();
    _UhfSockClient.instance = this;
  }
  setup() {
    try {
      this.driverInfo = JSON.parse(readFileSync("/var/uhf/uhf.var", "utf8"));
    } catch (error) {
      throw new UHFSocketError("UHF socket variable file not found. Please ensure that the UHF socket server is running and the /var/uhf/uhf.var file exists.");
    }
  }
  get client() {
    if (!this._client) {
      throw new UHFSocketError("Client is not initialized. Call start() first.");
    }
    return this._client;
  }
  get observable() {
    return this.subject.asObservable();
  }
  start() {
    if (!this.driverInfo) {
      throw new UHFSocketError("Driver info not available. Ensure that the UHF socket server is running and the /var/uhf/uhf.var file exists.");
    }
    this._client = net.createConnection(this.driverInfo.socketPath, () => {
      this.subject.next(new Message("CONNECTED" /* CONNECTED */, null));
    });
    this.client.on("data", (data) => {
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
        console.error("Error parsing JSON:", error);
      }
    });
    this.client.on("end", () => {
      this.subject.next(new Message("DISCONNECTED" /* DISCONNECTED */, null));
    });
  }
  stop() {
    if (this._client) {
      this.client.end();
      this._client = null;
      this.subject.next(new Message("DISCONNECTED" /* DISCONNECTED */, null));
    }
  }
  killProcess() {
    if (this.driverInfo?.pid) {
      try {
        process.kill(this.driverInfo.pid);
        this.subject.next(new Message("EXIT" /* EXIT */, null));
      } catch (error) {
        console.error("Error killing process:", error);
      }
    } else {
      throw new UHFSocketError("Driver PID not available. Ensure that the UHF socket server is running and the /var/uhf/uhf.var file exists.");
    }
  }
  sendMessage(message) {
    this.client.write(message.toJson() + "\n");
  }
  async getLogs(maxLines = 1e3) {
    if (!this.driverInfo) {
      throw new UHFSocketError("Driver info not available. Ensure that the UHF socket server is running and the /var/uhf/uhf.var file exists.");
    }
    try {
      const file = await fs.open(this.driverInfo.logsPath, "r");
      const stats = await file.stat();
      const fileSize = stats.size;
      const bufferSize = Math.min(fileSize, 5 * 1024 * 1024);
      const buffer = Buffer.alloc(bufferSize);
      await file.read(buffer, 0, bufferSize, fileSize - bufferSize);
      await file.close();
      const lines = buffer.toString().split("\n");
      return lines.slice(-maxLines).join("\n");
    } catch (error) {
      throw new UHFSocketError("Error reading logs: " + String(error));
    }
  }
};

// src/sock/send-events.enum.ts
var SendSockEvent = /* @__PURE__ */ ((SendSockEvent2) => {
  SendSockEvent2["DISCONNECTED"] = "DISCONNECTED";
  SendSockEvent2["START"] = "START";
  SendSockEvent2["STOP"] = "STOP";
  SendSockEvent2["RESET"] = "RESET";
  SendSockEvent2["SET_POWER"] = "SET_POWER";
  SendSockEvent2["GET_POWER"] = "GET_POWER";
  SendSockEvent2["SET_BEEP"] = "SET_BEEP";
  SendSockEvent2["GET_BEEP"] = "GET_BEEP";
  SendSockEvent2["EXIT"] = "EXIT";
  return SendSockEvent2;
})(SendSockEvent || {});

// src/@types/antenna.enum.ts
var Antenna = /* @__PURE__ */ ((Antenna2) => {
  Antenna2[Antenna2["ALL"] = 0] = "ALL";
  Antenna2[Antenna2["ANTENNA_1"] = 1] = "ANTENNA_1";
  Antenna2[Antenna2["ANTENNA_2"] = 2] = "ANTENNA_2";
  Antenna2[Antenna2["ANTENNA_3"] = 3] = "ANTENNA_3";
  Antenna2[Antenna2["ANTENNA_4"] = 4] = "ANTENNA_4";
  return Antenna2;
})(Antenna || {});

// src/index.ts
var _UhfSocket = class _UhfSocket {
  constructor(setup) {
    this.setup = setup;
    this.connection = new UhfSockClient();
    if (_UhfSocket.instance) {
      return _UhfSocket.instance;
    }
    _UhfSocket.instance = this;
  }
  inicialice() {
    if (_UhfSocket.started) {
      return;
    }
    _UhfSocket.started = true;
    this.connection.start();
    this.send("RESET" /* RESET */, null);
    this.send("SET_BEEP" /* SET_BEEP */, this.setup.beep);
    this.send("SET_POWER" /* SET_POWER */, {
      power: this.setup.power,
      antenna: 0 /* ALL */
    });
  }
  stop() {
    this.connection.stop();
    _UhfSocket.subscriptions.forEach((subscription) => subscription.unsubscribe());
    _UhfSocket.subscriptions = [];
    _UhfSocket.started = false;
  }
  get observable() {
    return this.connection.observable;
  }
  send(event, data) {
    const message = new Message(event, data);
    this.connection.sendMessage(message);
  }
  on(event, callback) {
    const subscription = this.connection.observable.subscribe((message) => {
      if (message.event === event) {
        callback(message);
      }
    });
    _UhfSocket.subscriptions.push(subscription);
    return subscription;
  }
  onAll(callback) {
    const subscription = this.connection.observable.subscribe((message) => {
      callback(message);
    });
    _UhfSocket.subscriptions.push(subscription);
    return subscription;
  }
  killProcess() {
    this.connection.killProcess();
    this.stop();
  }
  async getLogs(maxLines = 1e3) {
    return await this.connection.getLogs(maxLines);
  }
};
_UhfSocket.subscriptions = [];
_UhfSocket.started = false;
var UhfSocket = _UhfSocket;
var index_default = UhfSocket;
export {
  Antenna,
  SendSockEvent,
  SockEvent,
  UHFSocketError,
  index_default as default
};
