import { Drivers, UhfSocket } from "./dist/index.mjs";
import { SockEvent } from "./dist/index.mjs";

const socket = new UhfSocket(Drivers.SERIAL_H10);

(async () => {
    console.log("Starting socket...");
    await socket.inicialice();
    socket.on(SockEvent.SET_POWER, (message) => {
        console.log("Received message:", message);
    });
    socket.on(SockEvent.GET_POWER, (message) => {
        console.log("Received message:", message);
    });
    socket.on(SockEvent.TAG, (message) => {
        console.log("Received TAG:", message);
    });
    socket.send(SockEvent.SET_POWER, { antenna: 0, power: 15 });
    await new Promise((resolve) => setTimeout(resolve, 10000));
})();
