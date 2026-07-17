import UhfSocket from './dist/index.mjs';
import { SockEvent, Antenna } from './dist/index.mjs';

const socket = new UhfSocket({
  beep: false,
  power: 30
});


console.log('Starting socket...');
socket.inicialize();
socket.on(SockEvent.TAG, (tag) => {
  console.log(tag);
});