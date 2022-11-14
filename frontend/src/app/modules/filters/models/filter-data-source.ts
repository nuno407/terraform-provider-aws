import { CollectionViewer, DataSource } from '@angular/cdk/collections';
import { BehaviorSubject, Observable } from 'rxjs';

export class FilterDataSource<T> implements DataSource<T> {
  data = [
    {
      recordingInfoId: 'cae2bd19-38f1-4d9f-8b09-6632fa793c4d',
      startTimestamp: 1621501332000,
      endTimestamp: 1621503252000,
      incidentId: '6513c102-82fc-43e8-8938-7406443b7171',
      uploadId: '8e80c179-a80d-45b2-a16b-9501c71ffdd6',
      labeled: 'APP.no',
      url: 'assets/demo1_video.m4a',
    },
    {
      recordingInfoId: '2bbf4ec4-8ae9-4c7a-9a26-ce9fd195928a',
      startTimestamp: 1620588233000,
      endTimestamp: 1620589945000,
      incidentId: '478eff03-d8a8-42fe-9205-99d715fc3f02',
      uploadId: '0bc58c42-040b-4181-bddf-fdada9672386',
      labeled: 'APP.yes',
      url: 'assets/demo2_video.m4a',
    },
  ];

  connect(collectionViewer: CollectionViewer): Observable<T[] | readonly T[]> {
    return new BehaviorSubject<any[]>(this.data);
  }

  disconnect(collectionViewer: CollectionViewer): void {}
}
