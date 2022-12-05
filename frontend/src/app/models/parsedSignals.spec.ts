import { Signal, SignalGroup } from './parsedSignals';

describe('ParsedSignals', () => {
  it('should create signal', () => {
    let signalGroup = new SignalGroup('test-group');
    let signalValue = { x: new Date(), y: 1 };
    signalGroup.append('test-signal', signalValue);

    expect(signalGroup.name).toBe('test-group');
    expect(signalGroup.groups).toEqual([]);
    let signal = signalGroup.signals.find((signal) => signal.name == 'test-signal');
    expect(signal.values[0]).toEqual(signalValue);
  });

  it('should add to existing signal', () => {
    let signalGroup = new SignalGroup('test-group');
    let signalValue = { x: new Date(), y: 1 };
    signalGroup.append('test-signal', signalValue);
    let signalValue2 = { x: new Date(), y: 2 };
    signalGroup.append('test-signal', signalValue2);

    expect(signalGroup.name).toBe('test-group');
    expect(signalGroup.groups).toEqual([]);
    let signal = signalGroup.signals.find((signal) => signal.name == 'test-signal');
    expect(signal.values[0]).toEqual(signalValue);
    expect(signal.values[1]).toEqual(signalValue2);

    signalGroup.append('test-signal', signalValue);
  });
});
