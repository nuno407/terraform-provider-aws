export interface VideoSignals {
  message: { [name: string]: { [time: string]: { [signalName: string]: number | boolean } } };
}
