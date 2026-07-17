export class UHFSocketError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'UHFSocketError';
  }
}