import UhfSocket from "./dist/index.mjs";
import { SockEvent, Antenna } from "./dist/index.mjs";

const socket = new UhfSocket();

console.log("Starting socket...");
socket.inicialice();
socket.on(SockEvent.TAG, (tag) => {
    console.log(tag);
});
socket.onAll((error) => {
    console.error("Socket error:", error);
});
