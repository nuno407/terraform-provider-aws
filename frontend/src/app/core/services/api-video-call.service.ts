import { Message, RecordingOverviewObject, SingleObject, VideoInfo } from './../../models/recording-info';
import { Injectable } from '@angular/core';
import { HttpClient, HttpErrorResponse, HttpHeaders, HttpParams } from '@angular/common/http';
import { Observable, throwError } from 'rxjs';
import { environment } from '../../../environments/environment';

import { catchError, map, retry } from 'rxjs/operators';
import { ActivatedRoute } from '@angular/router';
import { VideoSignals } from 'src/app/models/video_signals';
import { SnapshotParser } from 'src/app/models/snapshots';

@Injectable({
  providedIn: 'root',
})
export class ApiVideoCallService {
  /**REST API BASE URL*/
  url = '';

  /**END POINTS */
  endPGetTableData = 'getTableData';
  endPGetVideoSignals = 'getVideoSignals';

  endPVideoUrl = 'getAnonymizedVideoUrl';
  endPDescription = 'videoDescription';
  endPbucket = 'bucket';
  endPfolder = 'folder';
  endPfile = 'file';

  /**String Message */
  message = '';

  constructor(private http: HttpClient, private route: ActivatedRoute) {}

  /*========================================
    CRUD Methods for consuming RESTful API
  =========================================*/

  // Http Options
  httpOptions = {
    headers: new HttpHeaders({
      'Content-Type': 'application/json',
    }),
  };

  getData(
    pageSize: number,
    page: number,
    queryParam: string,
    queryLogicOperator: string,
    sortingParam: string,
    sortingDirection: string
  ): Observable<RecordingOverviewObject> {
    this.url = this.getBackendUrl();
    return this.http
      .post<RecordingOverviewObject>(
        this.url + this.endPGetTableData,
        {
          query: queryParam,
          logic_operator: queryLogicOperator,
          sorting: sortingParam,
          direction: sortingDirection,
        },
        {
          params: new HttpParams({ fromObject: { size: pageSize, page: page } }),
        }
      )
      .pipe(retry(1), catchError(this.processError));
  }

  getSingleData(id: string): Observable<Message> {
    this.url = this.getBackendUrl();
    return this.http.get<SingleObject>(this.url + this.endPGetTableData + '/' + id).pipe(
      retry(1),
      catchError(this.processError),
      map((ro) => ro.message),
      map((msg) => ApiVideoCallService.addSnapshots(msg))
    );
  }

  private static addSnapshots(message: Message): Message {
    let videoStartTimestamp = message.time;
    message.parsed_snapshots = SnapshotParser.parse(message.snapshots_paths, videoStartTimestamp);
    return message;
  }

  /**add get video url + arguments s3_path to each items on getallitems */
  getVideo(id: string): Observable<string> {
    this.url = this.getBackendUrl();
    return this.http.get<VideoInfo>(this.url + this.endPVideoUrl + '/' + id).pipe(
      retry(1),
      catchError(this.processError),
      map((ro) => ro.message)
    );
  }

  setDescription(id: string, description: string): Observable<void> {
    this.url = this.getBackendUrl();
    return this.http.put<void>(this.url + this.endPDescription + '/' + id, { description: description }).pipe(retry(1), catchError(this.processError));
  }

  //get synchronized video signals endpoint
  getSyncVideoSignals(id: string): Observable<VideoSignals> {
    this.url = this.getBackendUrl();
    return this.http.get<VideoSignals>(`${this.url}${this.endPGetVideoSignals}/${id}`).pipe(retry(1), catchError(this.processError));
  }

  getBackendUrl(): string {
    return environment.api;
  }

  /**Add error handling */
  processError(err: HttpErrorResponse) {
    let message: any = '';
    if (err.error instanceof ErrorEvent) {
      // client error
      message = err.error.message;
    } else {
      // server error
      message = { errorCode: err.status, errorMessage: err.message, body: err.error };
    }
    console.log(message);
    return throwError(message);
  }
}
