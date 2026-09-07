import { Drivers, UhfSocket } from "./dist/index.mjs";
import { SockEvent } from "./dist/index.mjs";

const socket = new UhfSocket(Drivers.SERIAL_H10);

(async () => {
    console.log("Starting socket...");
    await socket.inicialice();
    console.log("Socket initialized");
    socket.on(SockEvent.TAG, (message) => {
        console.log("Received TAG:", message);
    });
    socket.send(SockEvent.RESET, null);
    await socket.stop();
    const socket2 = new UhfSocket(Drivers.SERIAL_H10);
    await socket2.inicialice();
    socket2.on(SockEvent.TAG, (message) => {
        console.log("Received TAG on socket2:", message);
    });
    await new Promise((resolve) => setTimeout(resolve, 100000));
})();
