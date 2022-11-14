import { AfterViewInit, Component, EventEmitter, Input, OnDestroy, Output, SecurityContext, ViewChild } from '@angular/core';
import { NotificationService } from '@bci-web-core/core';
import { Clipboard } from '@angular/cdk/clipboard';
import { MatRadioChange } from '@angular/material/radio';
import { MatSliderChange } from '@angular/material/slider';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import { fromEvent, interval, Subscription } from 'rxjs';
import { filter, finalize, map, takeUntil, takeWhile, throttleTime } from 'rxjs/operators';
import { LineChartComponent } from 'src/app/components/line-chart/line-chart.component';
import { ApiVideoCallService } from 'src/app/core/services/api-video-call.service';
import { LabelingService } from 'src/app/core/services/labeling.service';
import { SignalsRetrieverService } from 'src/app/core/services/signals-retriever.service';
import { ChartVideoScaling } from 'src/app/models/chartVideoScaling';
import { Label } from 'src/app/models/label';
import { SignalGroup } from 'src/app/models/parsedSignals';
import { Message } from 'src/app/models/recording-info';
import { Snapshot } from 'src/app/models/snapshots';
import { PlayState } from './play-state.enum';
import { MatCheckboxChange } from '@angular/material/checkbox';
@Component({
  selector: 'app-video-player',
  templateUrl: './video-player.component.html',
  styleUrls: ['./video-player.component.scss'],
})
export class VideoPlayerComponent implements AfterViewInit, OnDestroy {
  @Input() fps: number;

  _recording: Message;
  @Input()
  set recording(recording: Message) {
    this._recording = recording;

    // Get the signals and pass them to their selector
    this.signalRetriever.getSignals(this._recording._id, this._recording.lq_video?.id).subscribe((signals) => {
      console.log('Got signals from backend and writing them to the signal selector.');
      this.signalsToSelect = signals;
    });

    // Pass the snapshots to their selector
    this.snapshots = this._recording.parsed_snapshots;
  }
  get recording(): Message {
    return this._recording;
  }

  @Output() frame = new EventEmitter<number>();

  @ViewChild('video') videoElement;
  @ViewChild('lineChart') lineChart: LineChartComponent;
  @ViewChild('verticalBar') verticalBarElem;

  signalsToSelect: SignalGroup;
  snapshots: Snapshot[];

  verticalBar: HTMLDivElement;
  video: HTMLVideoElement;

  reverseSubscription: Subscription;
  keyboardSubscription: Subscription;

  playbackRate: number = 1;

  totalTime: number = 0;
  chartTotalTime: number = 0;
  chartToVideoFactor: number = 1;
  totalFrames: number = 0;
  currentTime: number = 0;
  currentFrame: number = 0;
  currentPercent: ChartVideoScaling = new ChartVideoScaling();
  playheaderPercentage: number = 2;
  playheaderHidden: boolean = true;
  currentZoomWindowStart: number = 0;
  currentZoomWindowEnd: number = 100;
  maxStartPercentage: number = 0;

  playState: PlayState = PlayState.Pause;
  route: any;

  saveDescriptionButtonHidden: boolean = true;
  saveDescriptionButtonDisabled: boolean = false;

  get playStates(): typeof PlayState {
    return PlayState;
  }

  mouseUp$ = fromEvent(document, 'mouseup');

  labels: any[];
  selectedLabelIndex: number = -1;

  /**variables to load on UI */
  url: string;
  public videoSrc: string;

  safeSrc: SafeResourceUrl;

  constructor(
    private labelingService: LabelingService,
    private metaDataApiService: ApiVideoCallService,
    private sanitizer: DomSanitizer,
    private signalRetriever: SignalsRetrieverService,
    private clipboard: Clipboard,
    private notify: NotificationService
  ) {
    this.labelingService.getLabels().subscribe((labels) => this.drawLabels(labels));
    this.labelingService.getSelectedLabelIndex().subscribe((index) => (this.selectedLabelIndex = index));
  }

  ngAfterViewInit() {
    this.video = this.videoElement.nativeElement;
    this.verticalBar = this.verticalBarElem.nativeElement;

    /**Call api service method */
    this.getvideo(); //antes passava o this.recording

    /* istanbul ignore next */
    this.video.ondurationchange = () => {
      this.totalTime = this.video.duration;
      this.totalFrames = this.totalTime * this.fps;
      this.updateScalingVideoToDiagram();
    };

    this.currentPercent.chartPercentageChange.subscribe(() => {
      const time = (this.video.duration * this.currentPercent.videoPercentage) / 100.0;
      this.video.currentTime = time;
    });

    /* istanbul ignore next */

    this.video.ontimeupdate = () => {
      this.currentTime = this.video.currentTime;
      this.currentFrame = this.video.currentTime * this.fps;
      this.currentPercent.videoPercentage = (100.0 * this.video.currentTime) / this.video.duration;
      this.frame.emit(this.currentFrame);
      this.updatePlayheaderPosition();
    };

    /* istanbul ignore next */
    this.video.onended = () => {
      this.playState = PlayState.Pause;
    };

    const mappedKeys = {
      Space: () => this.togglePlay(),
      ArrowLeft: () => this.seekFrames(-1),
      ArrowRight: () => this.seekFrames(1),
    };
    // todo - disable subscription if text-field is selected
    this.keyboardSubscription = fromEvent(document, 'keydown')
      .pipe(
        filter((e) => document.activeElement.tagName.toLowerCase() != 'textarea'),
        filter((e: KeyboardEvent) => Object.keys(mappedKeys).includes(e.code)),
        map((e: KeyboardEvent) => {
          e.stopPropagation();
          e.preventDefault();
          return mappedKeys[e.code];
        })
      )
      .subscribe((action) => {
        action();
      });
  }

  updatePlayheaderPosition() {
    if (this.currentPercent.chartPercentage > this.currentZoomWindowEnd || this.currentPercent.chartPercentage < this.currentZoomWindowStart) {
      this.playheaderHidden = true;
    } else {
      this.playheaderHidden = false;

      var leftOffsetPercent = this.lineChart.labelSizePercentage * 100;
      var percentualPosition = (this.currentPercent.chartPercentage - this.currentZoomWindowStart) / (this.currentZoomWindowEnd - this.currentZoomWindowStart);
      var chartSizePercent = this.lineChart.chartSizePercentage;
      var halfBarWidth = (this.verticalBar.clientWidth / this.verticalBar.parentElement.clientWidth) * 100;

      this.playheaderPercentage = leftOffsetPercent + percentualPosition * chartSizePercent * 100 - halfBarWidth;
    }
  }

  updateScalingVideoToDiagram() {
    this.currentPercent.chartToVideoFactor = this.chartTotalTime / this.totalTime;
  }

  ngOnDestroy() {
    if (this.keyboardSubscription) {
      this.keyboardSubscription.unsubscribe();
    }
    if (this.reverseSubscription) {
      this.reverseSubscription.unsubscribe();
    }
  }

  isSnapshotsOn(): boolean {
    return this.snapshots.every((elm) => elm.enabled);
  }

  changeSnapshots(event: MatCheckboxChange): void {
    for (let snap of this.snapshots) {
      snap.enabled = event.checked;
    }
    this.lineChart.snapshots = this.snapshots;
  }

  /* subscribe service to get the video throught S3 bucket*/
  getvideo() {
    return this.metaDataApiService.getVideo(this._recording._id).subscribe(
      (data) => {
        this.url = data;
        this.safeSrc = this.sanitizer.sanitize(SecurityContext.RESOURCE_URL, this.sanitizer.bypassSecurityTrustResourceUrl(this.url));
      },
      (error) => {
        console.log('Error ocurred: ', error);
      }
    );
  }

  togglePlay() {
    if (!this.video.paused || this.playState === PlayState.PlayReverse) {
      this.stopReverse();
      this.video.pause();
      this.playState = PlayState.Pause;
    } else {
      this.video.play();
      this.playState = PlayState.PlayForward;
    }
  }

  playHeaderClick(event) {
    let clickedPointElements = this.lineChart.chart.getElementsAtEventForMode(event, 'nearest', { intersect: true, includeInvisible: false }, false);
    if (clickedPointElements.length > 0) {
      let snapshot: Snapshot = clickedPointElements.filter((elem) => elem.element['$context'].raw.data ?? false)[0].element['$context'].raw.data;
      if (snapshot) {
        let snapshotNameWithoutExtension = snapshot.name.substring(0, snapshot.name.lastIndexOf('.'));
        this.clipboard.copy(snapshotNameWithoutExtension);
        let message = `Copied snapshot name to clipboard: ${snapshotNameWithoutExtension}`;
        console.info(message);
        this.notify.success(message);
        return;
      }
    }

    if (event.layerY > 30) {
      // prevent selection of text, drag/drop, etc.
      event.preventDefault();
      const width = event.target.getBoundingClientRect().width;
      const percentageAtStart = this.currentZoomWindowStart;
      const zoomFactor = (this.currentZoomWindowEnd - this.currentZoomWindowStart) / 100.0;
      this.currentPercent.chartPercentage =
        percentageAtStart +
        (100.0 / (width * (1 - this.lineChart.labelSizePercentage))) * (event.offsetX - this.lineChart.labelSizePercentage * width) * zoomFactor;
    }
  }

  zoomChangedByChart(newZoomWindow: [number, number]) {
    this.currentZoomWindowStart = newZoomWindow[0];
    this.currentZoomWindowEnd = newZoomWindow[1];
    this.maxStartPercentage = 100.0 - (this.currentZoomWindowEnd - this.currentZoomWindowStart);
    this.updatePlayheaderPosition();
  }

  slideScrollBar(event: MatSliderChange) {
    var zoomWindowSize = this.currentZoomWindowEnd - this.currentZoomWindowStart;
    let newZoomRange: [number, number] = [event.value, event.value + zoomWindowSize];
    this.lineChart.zoomRange = newZoomRange;

    // sliding does not raise the zoom changed event
    this.zoomChangedByChart(newZoomRange);
  }

  /* istanbul ignore next */
  seekFrames(numberOfFrames: number) {
    this.stopReverse();

    if (!this.video.paused) {
      this.video.pause();
      this.playState = PlayState.Pause;
    }

    const currentFrame = this.video.currentTime * this.fps;
    const newTime = (currentFrame + numberOfFrames) / this.fps;
    this.video.currentTime = newTime;
  }

  /* istanbul ignore next */
  playbackRateChange(event: MatRadioChange) {
    const rate = event.value;

    if (rate > 0) {
      this.video.playbackRate = rate;
    }
    this.playbackRate = event.value;

    if (this.playState === PlayState.PlayReverse) {
      this.reverse();
    }
  }

  /* istanbul ignore next */
  reverse() {
    this.stopReverse();

    if (!this.video.paused) {
      this.video.pause();
    }

    this.reverseSubscription = interval(1000 / this.playbackRate / this.fps)
      .pipe(
        takeWhile(() => this.video.currentTime > 0),
        finalize(() => {
          this.playState = PlayState.Pause;
        })
      )
      .subscribe((value) => {
        if (this.playState !== PlayState.PlayReverse) {
          this.playState = PlayState.PlayReverse;
        }
        const currentFrame = this.video.currentTime * this.fps;
        const newTime = (currentFrame - 1) / this.fps;
        this.video.currentTime = newTime;
      });

    this.playState = PlayState.PlayReverse;
  }

  forward() {
    this.stopReverse();
    this.togglePlay();
  }

  /* istanbul ignore next */
  cursorMove(event, cursor) {
    event.preventDefault();

    const width = event.target.parentElement.getBoundingClientRect().width;
    const barLeft = event.target.parentElement.getBoundingClientRect().left;
    const barRight = event.target.parentElement.getBoundingClientRect().right;
    let pageX = event.pageX;

    fromEvent(document, 'mousemove')
      .pipe(throttleTime(25), takeUntil(this.mouseUp$))
      .subscribe((moveEvent: MouseEvent) => {
        const newPageX = Math.min(Math.max(barLeft, moveEvent.pageX), barRight);
        const movementX = newPageX - pageX;
        const shift = (this.video.duration / width) * movementX;

        let label = this.labelingService.getLabel(this.selectedLabelIndex);
        if (cursor == 0) {
          label.start.seconds = Math.min(Math.max(0, label.start.seconds + shift), this.video.duration);
          label.start.frame = label.start.seconds * this.fps;
        } else if (cursor == 1) {
          label.end.seconds = Math.max(Math.min(this.video.duration, label.end.seconds + shift), 0);
          label.end.frame = label.end.seconds * this.fps;
        }
        this.labelingService.updateLabel(this.selectedLabelIndex, label);

        pageX = newPageX;
      });
  }

  getLabelStyle(label) {
    return { width: label.width + '%', left: label.start + '%' };
  }

  private drawLabels(labels: Label[]) {
    this.labels = [];
    labels.forEach((label) => {
      const start = (100 / this.video.duration) * label.start.seconds;
      const end = (100 / this.video.duration) * label.end.seconds;
      const width = end - start;

      this.labels.push({ start: start, end: end, width: width, visibility: label.visibility });
    });
  }

  private stopReverse() {
    if (this.reverseSubscription != undefined && !this.reverseSubscription.closed) {
      this.reverseSubscription.unsubscribe();
    }
  }

  public get Description(): string {
    return this._recording.description ?? '';
  }

  //Change the value of Hidden from true to false so the button Save can be shown
  public set Description(newDescription: string) {
    this._recording.description = newDescription;
    this.saveDescriptionButtonHidden = false;
  }

  saveDescription() {
    this.saveDescriptionButtonDisabled = true;
    this.metaDataApiService.setDescription(this._recording._id, this._recording.description).subscribe(() => {
      this.saveDescriptionButtonHidden = true;
      this.saveDescriptionButtonDisabled = false;
    });
  }
}
