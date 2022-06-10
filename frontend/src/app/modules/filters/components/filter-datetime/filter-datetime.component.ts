import { Component, OnInit, AfterViewChecked } from '@angular/core';
import { FilterType } from '../../models/filter-type.enum';
import { TranslateService } from '@ngx-translate/core';
import { FilterBaseComponent } from '../filter-base/filter-base.component';

@Component({
  selector: 'app-filter-datetime',
  templateUrl: './filter-datetime.component.html',
  styleUrls: ['./filter-datetime.component.scss'],
})
export class FilterDatetimeComponent extends FilterBaseComponent implements OnInit, AfterViewChecked {
  constructor(translateService: TranslateService) {
    super(translateService);
  }

  /**Aplication state */
  ngOnInit(): void {
    this.filterEvent.columnName = this.configuration.columnName;
    this.filterEvent.filterType = FilterType.DateTimeFilter;
    this.filterEvent.isActive = false;

    this.updateTranslations();
    this.translateService.onLangChange.subscribe(() => {
      this.updateTranslations();
    });
  }

  ngAfterViewChecked() {
    // ::ng-deep and :host is deprecated to style shadow doms, that's why this method is used.
    let datePicker = document.querySelector('.bci-datetime-picker');
    if (datePicker != null) {
      let htmlElement = <HTMLElement>datePicker.shadowRoot.querySelector('.bci-core-date-time-picker-container');
      if (htmlElement != null) {
        htmlElement.style['left'] = 'unset';
        htmlElement.style['right'] = '0';
      }
    }
  }

  onKey(event: any) {
    this.filterEvent.filterValue = event.target.value;
    this.newFilterEvent.emit(this.filterEvent);
  }

  getTimestampFromString(dateString: string, timeString: string) {
    let parts = dateString.split('.');
    let timeSuffix = timeString == null ? '' : timeString;
    return new Date(`${parts[2]}/${parts[1]}/${parts[0]} ${timeSuffix}`).getTime();
  }

  dateSelected(event) {
    if (event.detail.dateStart != null) {
      let startTime = this.getTimestampFromString(event.detail.dateStart, event.detail.timeStart);
      let endTime = this.getTimestampFromString(event.detail.dateEnd, event.detail.timeEnd);
      this.filterEvent.isActive = true;
      this.filterEvent.filterValue = { startTime: startTime, endTime: endTime };
    } else {
      this.filterEvent.isActive = false;
    }
    this.newFilterEvent.emit(this.filterEvent);
  }
}
