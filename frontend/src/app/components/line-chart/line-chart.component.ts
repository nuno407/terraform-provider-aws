import { Component, EventEmitter, Input, Output } from '@angular/core';
import { ChartDataset } from 'chart.js';
import Chart from 'chart.js/auto';
import 'chartjs-adapter-date-fns';
import zoomPlugin from 'chartjs-plugin-zoom';
import { format } from 'date-fns';
import { SignalGroup } from '../../models/parsedSignals';
import { Snapshot } from '../../models/snapshots';
import { LineChartColorPicker } from '../../shared/LineChartColorPicker';

@Component({
  selector: 'app-line-chart',
  templateUrl: './line-chart.component.html',
  styleUrls: ['./line-chart.component.scss'],
})
export class LineChartComponent {
  @Input() fps: number;

  _snapshots: Snapshot[];

  @Input()
  set snapshots(snapshots: Snapshot[]) {
    this._snapshots = snapshots;
    if (this.chart && this.chart.data) {
      let chartDatasets = this.createChartDatasets(this._signals, snapshots);
      this.chart.data.datasets = chartDatasets;
      this.chart.update();
    }
  }

  _signals: SignalGroup;
  @Input()
  set signals(signals: SignalGroup) {
    this._signals = signals;
    let chartDatasets = this.createChartDatasets(signals, this._snapshots);
    this.chart.data.datasets = chartDatasets;
    this.chart.update();

    if (signals) {
      [this.minimumTimestamp, this.maximumTimestamp] = this.calcMinMaxTimestamp(signals);
      this.chart.options.plugins.zoom.limits.x = { min: this.minimumTimestamp, max: this.maximumTimestamp };
      let totalDuration = (this.maximumTimestamp - this.minimumTimestamp) / 1000.0;
      this.totalDurationChange.emit(totalDuration);
    }
  }

  @Input()
  get zoomRange(): [number, number] {
    let xMin = this.scaleToPercentage(this.chart.scales.x.min);
    let xMax = this.scaleToPercentage(this.chart.scales.x.max);
    return [xMin, xMax];
  }
  set zoomRange(value: [number, number]) {
    if (this.chart) {
      this.chart.options.scales.x.min = this.scaleToTimestamp(value[0]);
      this.chart.options.scales.x.max = this.scaleToTimestamp(value[1]);
      this.chart.update();
    }
  }

  @Output() zoomRangeChange = new EventEmitter<[number, number]>();
  @Output() totalDurationChange = new EventEmitter<number>();

  get labelSizePercentage(): number {
    return this.chart.chartArea.left / this.chart.chartArea.width;
  }

  get chartSizePercentage(): number {
    return this.chart.chartArea.width / (this.chart.chartArea.width + this.chart.chartArea.left);
  }

  /**Chart */
  chart: Chart<'line', {}[]>;
  minimumTimestamp: number = Infinity;
  maximumTimestamp: number = 0;
  defaultAnimation: any;

  scaleToTimestamp(percentage: number) {
    let timestampRange = this.maximumTimestamp - this.minimumTimestamp;
    let boundedPercentage = Math.max(0.0, Math.min(100.0, percentage));
    let relativeTimestamp = (boundedPercentage * timestampRange) / 100.0;
    return this.minimumTimestamp + relativeTimestamp;
  }

  scaleToPercentage(timestamp: number) {
    let relativeTimestamp = timestamp - this.minimumTimestamp;
    let timestampRange = this.maximumTimestamp - this.minimumTimestamp;
    return (100.0 * relativeTimestamp) / timestampRange;
  }

  createChartDatasets(signals: SignalGroup, snapshots: Snapshot[]): ChartDataset<'line', {}[]>[] {
    if (!signals) return [this.createSnapshotChartDataset(snapshots)];
    if (!snapshots) return [...this.createSignalsChartDatasets(signals)];
    return [...this.createSignalsChartDatasets(signals), this.createSnapshotChartDataset(snapshots)];
  }

  createSnapshotChartDataset(snapshots: Snapshot[]): ChartDataset<'line', {}[]> {
    let snapshotData = snapshots
      .filter((snap) => snap.enabled)
      .map((snapshot) => {
        return {
          x: snapshot.videoTime.getTime(),
          y: 0,
          data: snapshot,
        };
      });

    let data_object: ChartDataset<'line', {}[]> = { data: snapshotData };
    data_object.label = 'Snapshots';
    data_object.borderWidth = 0;
    data_object.pointBorderWidth = 1;
    data_object.pointRadius = 17;
    data_object.hoverBorderWidth = 0;
    data_object.pointHoverRadius = 17;
    data_object.pointHitRadius = 5;
    data_object.pointStyle = 'triangle';
    data_object.fill = false;
    data_object.pointBackgroundColor = '#ad9600';
    data_object.pointBorderColor = '#ad9600';
    data_object.order = 1; // make sure it is drawn on top of the other lines
    return data_object;
  }

  createSignalsChartDatasets(signals: SignalGroup): ChartDataset<'line', {}[]>[] {
    let chartDatasets: ChartDataset<'line', {}[]>[] = [];
    let colorPicker = new LineChartColorPicker();
    for (let group of signals.groups) {
      for (let signal of group.signals) {
        /**create label colors */
        let lineColor;
        if (group.name == 'MDF') {
          lineColor = colorPicker.getNextMdfColor();
        } else {
          lineColor = colorPicker.getNextChcColor();
        }
        if (!signal.enabled) continue; // reserve the colors for non-enabled signals anyway

        let plotName = group.name + ': ' + signal.name;

        /**dataset response */
        let data_object: ChartDataset<'line', {}[]> = {
          data: signal.values.map((value) => {
            return { x: value.x.getTime(), y: value.y };
          }),
        };
        data_object.label = plotName;
        data_object.borderWidth = 3;
        data_object.pointRadius = 2;
        data_object.pointHoverRadius = 2;
        data_object.pointHitRadius = 1;
        data_object.pointBorderWidth = 0.5;
        data_object.fill = false;
        data_object.backgroundColor = lineColor;
        data_object.pointBackgroundColor = lineColor;
        data_object.pointBorderColor = lineColor;
        data_object.borderColor = lineColor;
        data_object.order = 2; // make sure it is drawn below the snapshots
        chartDatasets.push(data_object);
      }
    }
    return chartDatasets;
  }

  calcMinMaxTimestamp(signalGroup: SignalGroup): [number, number] {
    let min = Infinity,
      max = 0;
    for (let group of signalGroup.groups) {
      let grpMin, grpMax;
      [grpMin, grpMax] = this.calcMinMaxTimestamp(group);
      if (min > grpMin) min = grpMin;
      if (max < grpMax) max = grpMax;
    }
    for (let signal of signalGroup.signals) {
      for (let value of signal.values) {
        let time = value.x.getTime();
        if (min > time) min = time;
        if (max < time) max = time;
      }
    }
    return [min, max];
  }

  enableAnimations() {
    this.chart.options.animation = this.defaultAnimation;
  }

  disableAnimations() {
    this.chart.options.animation = false;
  }

  ngOnInit() {
    Chart.register(zoomPlugin);

    this.chart = new Chart('canvas', {
      type: 'line',
      data: { datasets: [] },
      options: {
        parsing: false,
        maintainAspectRatio: false,
        interaction: {
          mode: 'index',
        },
        scales: {
          x: {
            type: 'time',
            time: {
              displayFormats: {
                hour: 'HH:mm:ss',
                minute: 'mm:ss',
                second: 'mm:ss',
              },
              minUnit: 'second',
            },
          },
          y: {
            suggestedMin: 0,
            suggestedMax: 1,
          },
        },
        plugins: {
          zoom: {
            pan: {
              enabled: true,
              mode: 'x',
            },
            zoom: {
              mode: 'x',
              wheel: {
                enabled: true,
              },
              pinch: {
                enabled: true,
              },
              onZoomComplete: (c) => {
                this.zoomRangeChange.emit(this.zoomRange);
              },
            },
            limits: {
              x: {
                min: this.minimumTimestamp,
                max: this.maximumTimestamp,
              },
            },
          },
          tooltip: {
            callbacks: {
              title: function (context) {
                let title = '';
                if (context[0].dataset.label === 'Snapshots') {
                  title = 'Snapshot at ';
                }
                let date = new Date(context[0].parsed.x);
                if (date.getHours() > 0) {
                  title += format(date, 'HH:mm:ss,SSS');
                } else {
                  title += format(date, 'mm:ss,SSS');
                }
                return title;
              },
              label: function (context) {
                if (context.dataset.label === 'Snapshots') {
                  return null;
                } else {
                  return context.dataset.label + ': ' + context.formattedValue;
                }
              },
              afterBody: function (context) {
                if (context[0].dataset.label === 'Snapshots') {
                  let snapshotObject = context[0].raw['data'];
                  let snapshotDescription = 'Recorded:\t' + snapshotObject.recordTime.toISOString() + '\n';
                  snapshotDescription += 'Name:\t' + snapshotObject.name + '\n';
                  snapshotDescription += 'Frame:\t' + snapshotObject.frame + '\n';
                  snapshotDescription += 'Click to copy ID to clipboard';
                  return snapshotDescription;
                }
              },
            },
          },
          legend: {
            labels: {
              filter: function (item, chart) {
                // Logic to remove a particular legend item goes here
                return item.text !== 'Snapshots';
              },
            },
          },
          decimation: {
            enabled: true,
            algorithm: 'lttb',
          },
        },
      },
    });
    this.defaultAnimation = this.chart.options.animation;
  }
}
