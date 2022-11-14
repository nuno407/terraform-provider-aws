export class SignalGroup {
  name: string;
  groups: SignalGroup[] = [];
  signals: Signal[] = [];

  append(signalName: string, value: SignalValue) {
    let signal = this.signals.find((sig) => sig.name == signalName);
    if (signal == undefined) {
      signal = new Signal(signalName);
      this.signals.push(signal);
    }
    signal.append(value);
  }

  constructor(name: string) {
    this.name = name;
  }
}

export class Signal {
  name: string;
  values: SignalValue[] = [];
  enabled: boolean = true;

  append(value: SignalValue) {
    this.values.push(value);
  }

  constructor(name: string) {
    this.name = name;
  }
}

export interface SignalValue {
  x: Date;
  y: number;
}
