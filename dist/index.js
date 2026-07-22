"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// src/index.ts
var index_exports = {};
__export(index_exports, {
  Antenna: () => Antenna,
  SendSockEvent: () => SendSockEvent,
  SockEvent: () => SockEvent,
  UHFSocketError: () => UHFSocketError,
  default: () => index_default
});
module.exports = __toCommonJS(index_exports);

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
var import_net = __toESM(require("net"));
var import_rxjs = require("rxjs");
var import_fs = require("fs");

// src/errors/uhf-sock.error.ts
var UHFSocketError = class extends Error {
  constructor(message) {
    super(message);
    this.name = "UHFSocketError";
  }
};

// src/sock/uhf-sock.client.ts
var fs = __toESM(require("fs/promises"));
var UhfSockClient = class _UhfSockClient {
  constructor() {
    this.subject = new import_rxjs.Subject();
    this._client = null;
    this.driverInfo = null;
    // utils
    this.retryAttempts = 0;
    this.dataBuffer = "";
    if (_UhfSockClient.instance) {
      return _UhfSockClient.instance;
    }
    this.setup();
    _UhfSockClient.instance = this;
  }
  setup() {
    try {
      this.driverInfo = JSON.parse(
        (0, import_fs.readFileSync)("/var/uhf/uhf.var", "utf8")
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
    this._client = import_net.default.createConnection(
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
  constructor() {
    this.connection = new UhfSockClient();
    if (_UhfSocket.instance) {
      return _UhfSocket.instance;
    }
    _UhfSocket.instance = this;
  }
  inicialice() {
    if (this.connection._client) {
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
    if (!this.connection._client) {
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
  get observable() {
    return this.connection.observable;
  }
  send(event, data) {
    const message = new Message(event, data);
    this.connection.sendMessage(message);
  }
  on(event, callback) {
    const subscription = this.connection.observable.subscribe(
      (message) => {
        if (message.event === event) {
          callback(message);
        }
      }
    );
    _UhfSocket.subscriptions.push(subscription);
    return subscription;
  }
  onAll(callback) {
    const subscription = this.connection.observable.subscribe(
      (message) => {
        callback(message);
      }
    );
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
var UhfSocket = _UhfSocket;
var index_default = UhfSocket;
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  Antenna,
  SendSockEvent,
  SockEvent,
  UHFSocketError
});
