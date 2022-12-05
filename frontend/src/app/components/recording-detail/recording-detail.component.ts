import { Component, Inject, OnInit } from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { ActivatedRoute } from '@angular/router';
import { fromEvent } from 'rxjs';
import { takeUntil, throttleTime } from 'rxjs/operators';
import { FPS } from 'src/app/constants';
import { ApiVideoCallService } from 'src/app/core/services/api-video-call.service';
import { Message, RootObject } from 'src/app/models/recording-info';

@Component({
  selector: 'app-recording-detail',
  templateUrl: './recording-detail.component.html',
  styleUrls: ['./recording-detail.component.scss'],
})
export class RecordingDetailComponent implements OnInit {
  /**Local variables */
  recording;
  currentFrame: number = 0;
  fps = FPS;
  videoPlayerWidthPercentage = 60;

  /**Event */
  mouseUp$ = fromEvent(document, 'mouseup');

  lastUpdate: string;
  status: string;
  incidentID: string;
  dateInfo: string;

  dataResponse: RootObject;

  format: any;

  constructor(
    private route: ActivatedRoute,
    private metaDataApiService: ApiVideoCallService,
    public dialogRef: MatDialogRef<RecordingDetailComponent>,
    @Inject(MAT_DIALOG_DATA) public recordingId: string
  ) {}

  ngOnInit(): void {
    this.metaDataApiService.getSingleData(this.recordingId).subscribe((details: Message) => {
      this.recording = details;
    });
  }

  /* istanbul ignore next */
  selectHandle(event) {
    event.preventDefault();

    const parentRect = event.target.parentElement.getBoundingClientRect();
    fromEvent(document, 'mousemove')
      .pipe(throttleTime(25), takeUntil(this.mouseUp$))
      .subscribe((moveEvent: MouseEvent) => {
        const currentX = Math.min(Math.max(moveEvent.pageX - parentRect.left, 0), parentRect.width);

        let widthPercentage = (100 / parentRect.width) * currentX;
        this.videoPlayerWidthPercentage = Math.min(Math.max(widthPercentage, 35), 65);
      });
  }

  getVideoPlayerWidth() {
    return `${this.videoPlayerWidthPercentage}%`;
  }
}
