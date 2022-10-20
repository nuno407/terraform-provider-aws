import { EventEmitter, Output } from '@angular/core';

export class ChartVideoScaling {
  chartToVideoFactor: number = 1;
  #chartPercentage: number = 0;

  chartPercentageChange = new EventEmitter<number>();

  get chartPercentage(): number {
    return this.#chartPercentage;
  }
  set chartPercentage(newValue: number) {
    let limitedValue = Math.min(100, Math.max(0, newValue));
    let limitedToVideo = Math.min(100 / this.chartToVideoFactor, limitedValue);
    this.#chartPercentage = limitedToVideo;
    this.chartPercentageChange.emit(this.#chartPercentage);
  }

  get videoPercentage(): number {
    return this.#chartPercentage * this.chartToVideoFactor;
  }
  set videoPercentage(newValue: number) {
    let limitedValue = Math.min(100, Math.max(0, newValue));
    let limitedToChart = Math.min(100, limitedValue / this.chartToVideoFactor);
    this.#chartPercentage = limitedToChart;
  }
}
