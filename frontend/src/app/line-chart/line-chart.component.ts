import { Component, EventEmitter, Input, Output } from '@angular/core';
import { Chart, ChartDataset } from 'chart.js';
import 'chartjs-adapter-date-fns';
import zoomPlugin from 'chartjs-plugin-zoom';
import { format } from 'date-fns';
import { Message } from 'src/app/models/recording-info';
import { SignalGroup } from '../models/parsedSignals';
import { LineChartColorPicker } from '../shared/LineChartColorPicker';



@Component({
  selector: 'app-line-chart',
  templateUrl: './line-chart.component.html',
  styleUrls: ['./line-chart.component.scss']
})
export class LineChartComponent {
  @Input() recording: Message;
  @Input() fps: number;
  @Input()
  set signals(signals: SignalGroup)
  {
    if(signals) {
      let chartDatasets = this.createChartDatasets(signals);
      this.chart.data.datasets = chartDatasets;
      this.chart.update();

      [this.minimumTimestamp, this.maximumTimestamp] = this.calcMinMaxTimestamp(signals);
      this.chart.options.plugins.zoom.limits.x = {min:this.minimumTimestamp, max: this.maximumTimestamp};
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
    if(this.chart) {
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
    let boundedPercentage = Math.max(0.0, Math.min(100.0, percentage))
    let relativeTimestamp = boundedPercentage * timestampRange / 100.0;
    return this.minimumTimestamp + relativeTimestamp;
  }

  scaleToPercentage(timestamp: number) {
    let relativeTimestamp = timestamp - this.minimumTimestamp;
    let timestampRange = this.maximumTimestamp - this.minimumTimestamp;
    return 100.0 * relativeTimestamp / timestampRange;
  }

  createChartDatasets(signals: SignalGroup) {
    let chartDatasets : ChartDataset<"line", {}[]>[] = [];
    let colorPicker = new LineChartColorPicker();
    for (let group of signals.groups) {
      for (let signal of group.signals) {
        /**create label colors */
        let lineColor;
        if(group.name == 'MDF') {
          lineColor = colorPicker.getNextMdfColor();
        } else {
          lineColor = colorPicker.getNextChcColor();
        }
        if(!signal.enabled) continue; // reserve the colors for non-enabled signals anyway

        let plotName = group.name + ': ' + signal.name;

        /**dataset response */
        let data_object : ChartDataset<"line", {}[]> = {data: signal.values}
        data_object.label = plotName;
        data_object.borderWidth = 3;
        data_object.pointBorderWidth = 0.5;
        data_object.fill = false;
        data_object.backgroundColor = lineColor;
        data_object.pointBackgroundColor = lineColor;
        data_object.pointBorderColor = lineColor;
        data_object.borderColor = lineColor;

        chartDatasets.push(data_object);
      }
    }
    return chartDatasets;
  }

  calcMinMaxTimestamp(signalGroup: SignalGroup): [number, number] {
    let min = Infinity, max = 0;
    for(let group of signalGroup.groups) {
      let grpMin, grpMax;
      [grpMin, grpMax] = this.calcMinMaxTimestamp(group);
      if(min>grpMin) min = grpMin;
      if(max<grpMax) max = grpMax;
    }
    for(let signal of signalGroup.signals) {
      for(let value of signal.values) {
        let time = value.x.getTime();
        if(min>time) min = time;
        if(max<time) max = time;
      }
    }
    return[min, max];
  }

  enableAnimations() {
    this.chart.options.animation = this.defaultAnimation;
  }

  disableAnimations() {
    this.chart.options.animation = false;
  }

  ngOnInit() {
    Chart.register(zoomPlugin);
    this.chart = new Chart('canvas',
      {
        type: 'line',
        data: {datasets: []},
        options: {
          maintainAspectRatio: false,
          scales: {
            x: {
              type: 'time',
              time: {
                displayFormats: {
                  hour: "HH:mm:ss",
                  minute: "mm:ss",
                  second: "mm:ss"
                },
                minUnit: "second"
              }
            },
            y: {
              min: 0,
              max: 1
            }
          },
          plugins: {
            zoom: {
              pan: {
                enabled: true,
                mode: 'x'
              },
              zoom: {
                mode: 'x',
                wheel: {
                  enabled: true
                },
                pinch: {
                  enabled: true
                },
                onZoomComplete: (c) => { this.zoomRangeChange.emit(this.zoomRange) }
              },
              limits: {
                x: {
                  min: this.minimumTimestamp,
                  max: this.maximumTimestamp
                }
              }
            },
            tooltip: {
              callbacks: {
                  title: function(context) {
                    let date = new Date(context[0].parsed.x)
                    if (date.getHours() > 0) {
                      return format(date, "HH:mm:ss,SSS")
                    } else {
                      return format(date, "mm:ss,SSS")
                    }
                }
              }
            }
          }
        }
      });
      this.defaultAnimation = this.chart.options.animation;
  }

}
