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
  SockEvent2["DATA"] = "DATA";
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
    return Promise.resolve();
  }
  stop() {
    if (this._client) {
      this.client.end();
      this._client = null;
      this.emit("DISCONNECTED" /* DISCONNECTED */, null);
    }
    return Promise.resolve();
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
  async sendRaw(data) {
    this.client.write(data);
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

// src/driver/hexapad-10/hexapad-driver.ts
import { Subject as Subject2 } from "rxjs";
import { SerialPort } from "serialport";
import { promises as fs2 } from "fs";
import path from "path";

// src/driver/hexapad-10/commands/index.ts
var Command = class {
  constructor(driver, params) {
    this.driver = driver;
    this.params = params;
  }
  sendCommand(overrideCommand) {
    return new Promise((resolve, reject) => {
      this.driver.log(
        `Executing command: ${overrideCommand ?? this.command} ${this.params}`
      );
      const to = setTimeout(() => {
        reject(new UHFSocketError("Timeout waiting for response"));
        this.driver.log(
          `Command ${this.command} ${this.params} timed out.`
        );
        sub.unsubscribe();
      }, 5e3);
      this.driver.sendRaw(
        overrideCommand ?? `${this.command} ${this.params}`
      );
      const sub = this.driver.onAllRaw((data) => {
        clearTimeout(to);
        this.driver.log(`Received response: ${data}`);
        sub.unsubscribe();
        resolve(data);
      });
    });
  }
};

// src/driver/hexapad-10/commands/read-tag.ts
var ReadTag = class _ReadTag extends Command {
  constructor(driver, params) {
    super(driver, params);
    this.command = "readtag";
  }
  static execute(driver, params, silent = false) {
    const command = new _ReadTag(driver, params);
    return command.execute(silent);
  }
  async execute(silent = false) {
    try {
      const resp = await this.sendCommand();
      this.driver.subject.next(
        new Message(
          this.params === "on" ? "START" /* START */ : "STOP" /* STOP */,
          resp
        )
      );
      return resp;
    } catch (error) {
      this.driver.subject.next(
        new Message(
          "ERROR" /* ERROR */,
          error instanceof Error ? error.message : String(error)
        )
      );
      if (silent) {
        return "SILENT ERROR";
      }
      throw error;
    }
  }
};

// src/utils/exec-promise.ts
import { exec } from "child_process";
async function execPromise(command) {
  return new Promise((resolve, reject) => {
    exec(command, (error, stdout, stderr) => {
      if (error) {
        reject(error);
      } else {
        resolve(stdout);
      }
    });
  });
}

// src/driver/hexapad-10/utils/usb-port.ts
var USBPort = class _USBPort {
  static async getUSBPorts() {
    const usbPortInstance = new _USBPort();
    return usbPortInstance.findUSBPorts();
  }
  async findUnix() {
    try {
      const usbPortList = await execPromise("ls -a /dev/ | grep ttyACM0");
      const usbPort = usbPortList.split("\n")[0];
      if (!usbPort || usbPort.trim() === "") {
        return null;
      }
      return usbPort;
    } catch (error) {
      return null;
    }
  }
  async findMac() {
    try {
      const usbPortList = await execPromise(
        "ls -a /dev/ | grep cu.usbmodem"
      );
      const usbPort = usbPortList.split("\n")[0];
      if (!usbPort || usbPort.trim() === "") {
        return null;
      }
      return usbPort;
    } catch (error) {
      return null;
    }
  }
  async findUSBPorts() {
    const unix = await this.findUnix();
    if (unix) {
      return unix;
    }
    const mac = await this.findMac();
    if (mac) {
      return mac;
    }
    throw new UHFSocketError("No USB ports found");
  }
};

// src/driver/hexapad-10/commands/enable-beep.ts
var EnableBeep = class _EnableBeep extends Command {
  constructor(driver, params) {
    super(driver, params);
    this.command = "enablebeep";
  }
  static execute(driver, params, silent = false) {
    const command = new _EnableBeep(driver, params);
    return command.execute(silent);
  }
  async execute(silent = false) {
    try {
      await ReadTag.execute(this.driver, "off");
      const resp = await this.sendCommand(
        this.params === "get" ? this.command : `${this.command} ${this.params}`
      );
      this.driver.subject.next(
        new Message(
          this.params === "get" ? "GET_BEEP" /* GET_BEEP */ : "SET_BEEP" /* SET_BEEP */,
          resp.includes("on")
        )
      );
      return resp;
    } catch (error) {
      this.driver.subject.next(
        new Message(
          "ERROR" /* ERROR */,
          error instanceof Error ? error.message : String(error)
        )
      );
      if (silent) {
        return "SILENT ERROR";
      }
      throw error;
    } finally {
      await ReadTag.execute(this.driver, "on");
    }
  }
};

// src/driver/hexapad-10/commands/read-power.ts
var ReadPower = class _ReadPower extends Command {
  constructor(driver, params) {
    super(driver, params);
    this.command = "readpower";
  }
  static execute(driver, params, silent = false) {
    const command = new _ReadPower(driver, params);
    return command.execute(silent);
  }
  async execute(silent = false) {
    try {
      await ReadTag.execute(this.driver, "off");
      const resp = await this.sendCommand(
        this.params === 0 ? this.command : `${this.command} ${this.params}`
      );
      const power = Number(resp.replace(/\D/g, ""));
      this.driver.subject.next(
        new Message(
          this.params === 0 ? "GET_POWER" /* GET_POWER */ : "SET_POWER" /* SET_POWER */,
          [power, power, power, power]
          // to no broken the default interface for 4 antenna,
        )
      );
      return resp;
    } catch (error) {
      this.driver.subject.next(
        new Message(
          "ERROR" /* ERROR */,
          error instanceof Error ? error.message : String(error)
        )
      );
      if (silent) {
        return "SILENT ERROR";
      }
      throw error;
    } finally {
      await ReadTag.execute(this.driver, "on");
    }
  }
};

// src/driver/hexapad-10/hexapad-driver.ts
var HexapadDriver = class extends BaseDriver {
  constructor() {
    super();
    this.subject = new Subject2();
    this.name = "serial-h10" /* SERIAL_H10 */;
    this._port = null;
    this.subjectRaw = new Subject2();
    this.messageBuffer = "";
    this.sendMessagePipe = [];
    this.pipeCron = null;
    this.cronIsRunning = false;
    // logs-related methods are implemented below
    this.logsPath = path.join(process.cwd(), "logs", "hexapad.log");
  }
  get isRunning() {
    return !!this._port;
  }
  get port() {
    if (!this._port) {
      throw new UHFSocketError("Serial port is not initialized.");
    }
    return this._port;
  }
  startCron() {
    if (this.pipeCron) {
      clearInterval(this.pipeCron);
    }
    this.pipeCron = setInterval(async () => {
      if (this.cronIsRunning) {
        return;
      }
      try {
        const fn = this.sendMessagePipe.shift();
        if (fn) {
          this.cronIsRunning = true;
          console.log("Executing command from pipe...");
          await fn();
          console.log("Command executed.");
          this.cronIsRunning = false;
        }
      } catch (error) {
        this.cronIsRunning = false;
      }
    }, 100);
  }
  async start() {
    try {
      this.log("Starting HexapadDriver...");
      this.subject.complete();
      this.subjectRaw.complete();
      this.subject = new Subject2();
      this.subjectRaw = new Subject2();
      await this.createLogsFileIfNotExists();
      const usbPort = await USBPort.getUSBPorts();
      this._port = new SerialPort({
        path: `/dev/${usbPort}`,
        baudRate: 115200,
        dataBits: 8,
        stopBits: 1,
        parity: "none"
      });
      this.log(
        "HexapadDriver started.",
        `SERIAL PORT OPENED: /dev/${usbPort}`
      );
      this.port.on("data", (data) => {
        const chunk = data.toString();
        this.messageBuffer += chunk;
        if (chunk.includes("\r\n")) {
          const message = this.messageBuffer.replace(">", "");
          this.messageBuffer = "";
          this.subjectRaw.next(message);
          if (!this.cronIsRunning) {
            this.subject.next(
              new Message(
                "TAG" /* TAG */,
                message.replace("\r\n", "").toUpperCase()
              )
            );
          }
        }
      });
      this.port.on("error", (err) => {
        this.log("Serial port error: " + String(err));
      });
      this.startCron();
      this.log(await ReadTag.execute(this, "on"));
    } catch (error) {
      console.log(error);
      this.subject.next(new Message("ERROR" /* ERROR */, error));
    }
  }
  stopCron() {
    if (this.pipeCron) {
      clearInterval(this.pipeCron);
      this.pipeCron = null;
    }
  }
  async stop() {
    const resp = await ReadTag.execute(this, "off");
    this.stopCron();
    this.sendMessagePipe = [];
    this.log(resp);
  }
  send(event, data) {
    this.sendMessagePipe.push(
      () => this.sendMessagePipeResolver(event, data)
    );
  }
  async sendMessagePipeResolver(event, data) {
    switch (event) {
      case "STOP" /* STOP */:
        await ReadTag.execute(this, "off", true);
        break;
      case "START" /* START */:
        await ReadTag.execute(this, "on", true);
        break;
      case "DISCONNECTED" /* DISCONNECTED */:
        this.killProcess();
        break;
      case "EXIT" /* EXIT */:
        this.killProcess();
        break;
      case "GET_BEEP" /* GET_BEEP */:
        await EnableBeep.execute(this, "get", true);
        break;
      case "SET_BEEP" /* SET_BEEP */:
        await EnableBeep.execute(
          this,
          data === true ? "on" : "off",
          true
        );
        break;
      case "RESET" /* RESET */:
        await ReadTag.execute(this, "off", true);
        await ReadTag.execute(this, "on", true);
        break;
      case "GET_POWER" /* GET_POWER */:
        await ReadPower.execute(this, 0, true);
        break;
      case "SET_POWER" /* SET_POWER */:
        await ReadPower.execute(
          this,
          data?.power ?? 10,
          true
        );
        break;
      default:
        this.log("Unknown event: " + String(event));
        this.subject.next(
          new Message(
            "ERROR" /* ERROR */,
            "Unknown event: " + String(event)
          )
        );
        break;
    }
  }
  on(event, callback) {
    return this.subject.subscribe((message) => {
      if (message.event === event) {
        callback(message);
      }
    });
  }
  onAll(callback) {
    return this.subject.subscribe((message) => {
      callback(message);
    });
  }
  killProcess() {
    if (this._port) {
      this._port.close();
      this._port = null;
    }
    this.subject.next(
      new Message(
        "DISCONNECTED" /* DISCONNECTED */,
        "Hexapad driver process killed"
      )
    );
  }
  // Raw data handling methods
  /**
   * @param data The raw data string to send to the device.
   * @description Sends a raw data string directly to the device.
   */
  async sendRaw(data) {
    this.port.write(`
${data}\r
`);
  }
  /**
   * @param callback The callback function to handle raw messages.
   * @returns A subscription to the raw message stream.
   * @description Use only if you need to handle raw messages directly from the device.
   */
  onAllRaw(callback) {
    return this.subjectRaw.subscribe((message) => {
      callback(message);
    });
  }
  async log(...data) {
    try {
      const file = await fs2.open(this.logsPath, "a");
      await file.write(
        data.map((s) => `${(/* @__PURE__ */ new Date()).toISOString()} - ${s} 
`).join("")
      );
      await file.close();
    } catch (error) {
      throw new UHFSocketError(
        "Error appending to logs: " + String(error)
      );
    }
  }
  async createLogsFileIfNotExists() {
    const logsDir = path.dirname(this.logsPath);
    await fs2.mkdir(logsDir, { recursive: true });
    const file = await fs2.open(this.logsPath, "a", 438);
    await file.close();
  }
  async getLogs(maxLines = 1e3) {
    try {
      const file = await fs2.open(this.logsPath, "r");
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
      case "serial-h10" /* SERIAL_H10 */:
        this._connection = new HexapadDriver();
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
  async inicialice() {
    try {
      if (this.connection.isRunning) {
        throw new UHFSocketError(
          "UHF Socket is already started. Please stop it before initializing again."
        );
      }
      await this.connection.start();
      this.send("RESET" /* RESET */, null);
      this.on("DISCONNECTED" /* DISCONNECTED */, () => {
        _UhfSocket.subscriptions.forEach(
          (subscription) => subscription.unsubscribe()
        );
        _UhfSocket.subscriptions = [];
      });
    } catch (error) {
    }
  }
  async stop() {
    if (!this.connection.isRunning) {
      throw new UHFSocketError(
        "UHF Socket is not started. Please start it before stopping."
      );
    }
    await this.connection.stop();
    _UhfSocket.subscriptions.forEach(
      (subscription) => subscription.unsubscribe()
    );
    _UhfSocket.subscriptions = [];
  }
  send(event, data) {
    this.connection.send(event, data);
  }
  async sendRaw(data) {
    await this.connection.sendRaw(data);
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
export {
  Antenna,
  Drivers,
  SendSockEvent,
  SockEvent,
  UHFSocketError,
  UhfSocket
};
