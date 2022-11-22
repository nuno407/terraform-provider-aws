import { CdkVirtualScrollViewport } from '@angular/cdk/scrolling';
import { AfterViewInit, ChangeDetectorRef, Component, OnInit, ViewChild } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';
import { format, subDays } from 'date-fns';
import { ApiVideoCallService } from 'src/app/core/services/api-video-call.service';
import { Message } from 'src/app/models/recording-info';
import { RecordingDetailComponent } from '../recording-detail/recording-detail.component';

@Component({
  selector: 'app-recording-overview',
  templateUrl: './recording-overview.component.html',
  styleUrls: ['./recording-overview.component.scss'],
})
export class RecordingOverviewComponent implements OnInit, AfterViewInit {
  @ViewChild(CdkVirtualScrollViewport, { static: false })
  public viewPort: CdkVirtualScrollViewport;

  headerTop = '0px';
  dataResponse: any;
  dataSource: any;
  queryParam: string = '';
  queryLogicOperator: string = 'AND';
  sortingParam: string = '';
  sortingOrder: string = 'dsc';

  videoList: Message[] = [];

  pageSize: number = 20;
  page: number = 1;
  numberOfEntries: number = 1;
  isLoaded: boolean = false;
  error: string;

  filterHelpText: string =
    'Available fields:\n\
  - _id,\n\
  - processing_list,\n\
  - snapshots,\n\
  - data_status,\n\
  - last_updated,\n\
  - length,\n\
  - time,\n\
  - resolution,\n\
  - number_chc_events,\n\
  - lengthCHC,\n\
  - max_person_count,\n\
  - ride_detection_counter,\n\
  - deviceID\n\n\
  Available operands:\n\
  - ==\n\
  - >\n\
  - <\n\
  - !=\n\
  - has';

  orderHelpText: string =
    'Available fields:\n\
  - _id,\n\
  - processing_list,\n\
  - snapshots,\n\
  - data_status,\n\
  - last_updated,\n\
  - length,\n\
  - time,\n\
  - resolution,\n\
  - number_chc_events,\n\
  - lengthCHC,\n\
  - max_person_count,\n\
  - ride_detection_counter,\n\
  - deviceID';

  constructor(private changeDetectorRef: ChangeDetectorRef, private metadata: ApiVideoCallService, private dialog: MatDialog) {}

  ngOnInit(): void {
    this.retrieveRecordingList();
  }

  ngAfterViewInit(): void {
    if (!!this.viewPort) {
      this.viewPort['_scrollStrategy'].onRenderedOffsetChanged = () => {
        this.headerTop = `-${this.viewPort.getOffsetToRenderedContentStart()}px`;
        this.changeDetectorRef.detectChanges();
      };
    }
  }

  retrieveRecordingList() {
    this.isLoaded = false;
    this.error = undefined;
    this.metadata.getData(this.pageSize, this.page, this.queryParam.trim(), this.queryLogicOperator, this.sortingParam, this.sortingOrder).subscribe(
      (resp) => {
        this.numberOfEntries = resp.total;
        this.videoList = resp.message;
        this.isLoaded = true;
      },
      (err) => {
        if (err.body && err.body.message) {
          this.error = err.body.message;
        } else {
          this.error = err.errorMessage;
        }
      }
    );
  }

  upload(event: MouseEvent) {
    event.stopPropagation();
  }

  showDetailDialog(recordingId: string) {
    this.dialog.open(RecordingDetailComponent, {
      data: recordingId,
      height: 'calc(100vh - 90px)',
      width: 'calc(100vw - 90px)',
      maxWidth: '100%',
      maxHeight: '100%',
    });
  }

  filterLast() {
    let yesterdayISO = format(subDays(new Date(), 1), 'yyyy-MM-dd HH:mm:ss');
    let filter = "'time': { '>': \"" + yesterdayISO + '" }';
    this.addQuery(filter);
    this.retrieveRecordingList();
  }

  filterChcEvents() {
    let filter = "'number_chc_events': { '>': 0}";
    this.addQuery(filter);
    this.retrieveRecordingList();
  }

  /**method for clear input text */
  deleteInputQueryText() {
    this.queryParam = '';
    this.sortingParam = '';
  }

  addQuery(queryToAdd: string) {
    let newQuery = this.queryParam.trim();
    // strip away closing bracket at the end, if filter is bracketed
    if (newQuery.startsWith('{') && newQuery.endsWith('}')) {
      newQuery = newQuery.substring(1, newQuery.length - 2);
    }

    if (newQuery) {
      newQuery += ',' + queryToAdd;
    } else {
      newQuery = queryToAdd;
    }
    this.queryParam = newQuery;
  }

  sortBy(sortingParam) {
    if (this.sortingParam != sortingParam) {
      this.sortingParam = sortingParam;
      this.sortingOrder = 'asc';
    } else {
      if (this.sortingOrder == 'asc') {
        this.sortingOrder = 'dsc';
      } else {
        this.sortingOrder = 'asc';
        // do we want two or three stage clicking?
        //this.sortingParam = "";
      }
    }
    this.retrieveRecordingList();
  }
}
