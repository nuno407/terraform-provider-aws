import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { concatAll, map } from 'rxjs/operators';
import { SignalGroup } from 'src/app/models/parsedSignals';
import { VideoSignals } from 'src/app/models/video_signals';
import { ApiVideoCallService } from './api-video-call.service';

@Injectable({
  providedIn: 'root',
})
export class SignalsRetrieverService {
  constructor(private metaDataApiService: ApiVideoCallService) {}

  getSignals(recordingId: string, fallbackRecordingId: string = undefined): Observable<SignalGroup> {
    return this.metaDataApiService.getSyncVideoSignals(recordingId).pipe(
      map((response) => {
        if (!(response.message.MDF || response.message.MDFParser) && fallbackRecordingId) {
          console.log('Falling back to LQ MDF..');
          return this.metaDataApiService.getSyncVideoSignals(fallbackRecordingId).pipe(
            map((lq_response) => {
              if (lq_response.message.MDF) {
                response.message.MDF = lq_response.message.MDF;
                console.log('Acquired LQ MDF');
              } else if (lq_response.message.MDFParser) {
                response.message.MDFParser = lq_response.message.MDFParser;
                console.log('Acquired LQ MDF');
              }
              return response;
            })
          );
        } else {
          return of(response);
        }
      }),
      concatAll(),
      map((mdf) => SignalsRetrieverService.parseSignals(mdf))
    );
  }

  private static parseSignals(videoSignals: VideoSignals): SignalGroup {
    let datasets: SignalGroup = new SignalGroup('All');
    for (const [dataSetName, dataSet] of Object.entries(videoSignals.message)) {
      let signalGroup = new SignalGroup(dataSetName);
      datasets.groups.push(signalGroup);
      for (const [timeStr, signals] of Object.entries(dataSet)) {
        let time = SignalsRetrieverService.timeFromString(timeStr);
        for (const [signalName, value] of Object.entries(signals)) {
          signalGroup.append(signalName, { x: time, y: Number(value) });
        }
      }
    }
    return datasets;
  }

  private static timeFromString(timeStr: string): Date {
    let timeSplit = timeStr.split(/[\.\:\,]/);
    let hours = Number(timeSplit[0]);
    let minutes = Number(timeSplit[1]);
    let seconds = Number(timeSplit[2]);
    let ms = 0;
    if (timeSplit.length > 3) {
      ms = Number(timeSplit[3]) / 1000;
    }
    return new Date(2020, 1, 1, hours, minutes, seconds, ms);
  }
}
