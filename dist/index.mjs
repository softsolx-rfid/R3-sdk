// src/@types/antenna.enum.ts
var Antenna = /* @__PURE__ */ ((Antenna2) => {
  Antenna2[Antenna2["ALL"] = 0] = "ALL";
  Antenna2[Antenna2["ANTENNA_1"] = 1] = "ANTENNA_1";
  Antenna2[Antenna2["ANTENNA_2"] = 2] = "ANTENNA_2";
  Antenna2[Antenna2["ANTENNA_3"] = 3] = "ANTENNA_3";
  Antenna2[Antenna2["ANTENNA_4"] = 4] = "ANTENNA_4";
  return Antenna2;
})(Antenna || {});

// src/errors/uhf-sock.error.ts
var UHFSocketError = class extends Error {
  constructor(message) {
    super(message);
    this.name = "UHFSocketError";
  }
};

// src/driver/send-events.enum.ts
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

// src/driver/events.enum.ts
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

// src/driver/sock-r3/uhf-sock.driver.ts
import net from "net";
import { Subject } from "rxjs";
import { readFileSync } from "fs";
import * as fs from "fs/promises";

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

// src/driver/base-driver.abstract.ts
var BaseDriver = class {
};

// src/driver/sock-r3/uhf-sock.driver.ts
var UhfSockDriver = class _UhfSockDriver extends BaseDriver {
  constructor() {
    super();
    this.subject = new Subject();
    this._client = null;
    this.driverInfo = null;
    // utils
    this.retryAttempts = 0;
    this.dataBuffer = "";
    this.name = "uhf-socket-r3" /* UHF_SOCKET_R3 */;
    if (_UhfSockDriver.instance) {
      return _UhfSockDriver.instance;
    }
    this.setup();
    _UhfSockDriver.instance = this;
  }
  get isRunning() {
    return this._client !== null;
  }
  setup() {
    try {
      this.driverInfo = JSON.parse(
        readFileSync("/var/uhf/uhf.var", "utf8")
      );
    } catch (error) {
      throw new UHFSocketError(
        "UHF socket variable file not found. Please ensure that the UHF socket server is running and the /var/uhf/uhf.var file exists."
      );
    }
  }
  get client() {
    if (!this._client) {
      throw new UHFSocketError(
        "Client is not initialized. Call start() first."
      );
    }
    return this._client;
  }
  get observable() {
    return this.subject.asObservable();
  }
  start() {
    if (!this.driverInfo) {
      throw new UHFSocketError(
        "Driver info not available. Ensure that the UHF socket server is running and the /var/uhf/uhf.var file exists."
      );
    }
    this._client = net.createConnection(
      this.driverInfo.socketPath,
      () => this.emit("CONNECTED" /* CONNECTED */, null)
    );
    this.client.on("data", (data) => {
      if (data.toString() === "PING") {
        this.retryAttempts = 0;
        this.client.write("PONG\n");
        return;
      }
      try {
        this.dataBuffer += data.toString();
        let line = this.dataBuffer.indexOf("\n");
        while (line !== -1) {
          const messageStr = this.dataBuffer.slice(0, line);
          this.dataBuffer = this.dataBuffer.slice(line + 1);
          try {
            const message = JSON.parse(messageStr);
            this.emit(message.event, message.data);
          } catch (error) {
            this.emitError(
              "Error parsing message: " + String(error)
            );
          }
          line = this.dataBuffer.indexOf("\n");
        }
      } catch (error) {
        this.emitError("Error processing data: " + String(error));
      }
    });
    this.client.on("close", () => {
      this.stop();
    });
    this.client.on("error", (err) => {
      this.emitError("Socket error: " + String(err));
      this.retryConnection();
    });
    this.client.on("end", () => {
      this.stop();
    });
  }
  stop() {
    if (this._client) {
      this.client.end();
      this._client = null;
      this.emit("DISCONNECTED" /* DISCONNECTED */, null);
    }
  }
  sendMessage(message) {
    this.client.write(message.toJson() + "\n");
  }
  retryConnection() {
    if (this.retryAttempts < 5) {
      this.retryAttempts++;
      setTimeout(() => {
        this.start();
      }, 1e3 * this.retryAttempts);
    } else {
      this.stop();
      this.emitError("Max retry attempts reached. Connection failed.");
    }
  }
  emit(event, data) {
    this.subject.next(new Message(event, data));
  }
  emitError(message) {
    this.subject.next(
      new Message("ERROR" /* ERROR */, new UHFSocketError(message))
    );
  }
  // public methods from abstraction
  on(event, callback) {
    const subscription = this.observable.subscribe(
      (message) => {
        if (message.event === event) {
          callback(message);
        }
      }
    );
    return subscription;
  }
  onAll(callback) {
    const subscription = this.observable.subscribe(
      (message) => {
        callback(message);
      }
    );
    return subscription;
  }
  send(event, data) {
    const message = new Message(event, data);
    this.sendMessage(message);
  }
  // utils
  async getLogs(maxLines = 1e3) {
    if (!this.driverInfo) {
      throw new UHFSocketError(
        "Driver info not available. Ensure that the UHF socket server is running and the /var/uhf/uhf.var file exists."
      );
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
  killProcess() {
    if (this.driverInfo?.pid) {
      try {
        this.client.destroy();
        if (process.getuid && process.getuid() !== 0) {
          throw new UHFSocketError(
            "Insufficient privileges to kill the process. Please run the application with sudo or as root."
          );
        }
        process.kill(this.driverInfo.pid);
        this.subject.next(new Message("EXIT" /* EXIT */, null));
      } catch (error) {
        console.error("Error killing process:", error);
      }
    } else {
      throw new UHFSocketError(
        "Driver PID not available. Ensure that the UHF socket server is running and the /var/uhf/uhf.var file exists."
      );
    }
  }
};

// src/index.ts
var Drivers = /* @__PURE__ */ ((Drivers2) => {
  Drivers2["UHF_SOCKET_R3"] = "uhf-socket-r3";
  Drivers2["SERIAL_H10"] = "serial-h10";
  return Drivers2;
})(Drivers || {});
var _UhfSocket = class _UhfSocket {
  constructor(driver) {
    this._connection = null;
    if (_UhfSocket.instance) {
      return _UhfSocket.instance;
    }
    switch (driver) {
      case "uhf-socket-r3" /* UHF_SOCKET_R3 */:
        this._connection = new UhfSockDriver();
        break;
      default:
        throw new UHFSocketError("Unsupported driver");
    }
    _UhfSocket.instance = this;
  }
  get connection() {
    if (!this._connection) {
      throw new UHFSocketError(
        "Connection is not initialized. Call inicialice() first."
      );
    }
    return this._connection;
  }
  get isStarted() {
    return this.connection.isRunning;
  }
  inicialice() {
    if (this.connection.isRunning) {
      throw new UHFSocketError(
        "UHF Socket is already started. Please stop it before initializing again."
      );
    }
    this.connection.start();
    this.send("RESET" /* RESET */, null);
    this.on("DISCONNECTED" /* DISCONNECTED */, () => {
      _UhfSocket.subscriptions.forEach(
        (subscription) => subscription.unsubscribe()
      );
      _UhfSocket.subscriptions = [];
    });
  }
  stop() {
    if (!this.connection.isRunning) {
      throw new UHFSocketError(
        "UHF Socket is not started. Please start it before stopping."
      );
    }
    this.connection.stop();
    _UhfSocket.subscriptions.forEach(
      (subscription) => subscription.unsubscribe()
    );
    _UhfSocket.subscriptions = [];
  }
  send(event, data) {
    this.connection.send(event, data);
  }
  on(event, callback) {
    const sub = this.connection.on(event, callback);
    _UhfSocket.subscriptions.push(sub);
    return sub;
  }
  onAll(callback) {
    const sub = this.connection.onAll(callback);
    _UhfSocket.subscriptions.push(sub);
    return sub;
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
var UhfSocket = _UhfSocket;
var index_default = UhfSocket;
export {
  Antenna,
  Drivers,
  SendSockEvent,
  SockEvent,
  UHFSocketError,
  index_default as default
};
