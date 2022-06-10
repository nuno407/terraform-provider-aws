import { Component, EventEmitter, Input, Output } from '@angular/core';
import { TranslateService } from '@ngx-translate/core';
import { FilterEvent } from '../../models/filter-event';
import { StringFilterConfiguration } from '../../models/string-filter-configuration';

@Component({
  template: '',
})
export abstract class FilterBaseComponent {
  @Input() configuration: StringFilterConfiguration;
  @Output() newFilterEvent = new EventEmitter<FilterEvent<string>>();

  /**Local variables */
  filterLabel = 'All';
  filterKey = 'FILTER.all';

  radioAllId = 'radio-all';
  radioNotSelectedId = 'radio-not-selected';
  radioContainsId = 'radio-contains';
  radioCheckedId = this.radioAllId;

  filterEvent = new FilterEvent<any>();

  constructor(protected translateService: TranslateService) {}

  radioClick = (id: string) => {
    this.radioCheckedId = id;
    this.filterKey = id != this.radioAllId ? 'FILTER.filterOne' : 'FILTER.all';
    this.updateTranslations();

    if (id === this.radioContainsId) {
      this.filterEvent.isNotSelected = false;
      this.filterEvent.isActive = true;
    } else if (id === this.radioAllId) {
      this.filterEvent.isActive = false;
      this.filterEvent.isNotSelected = false;
    } else if (id === this.radioNotSelectedId) {
      this.filterEvent.isActive = true;
      this.filterEvent.isNotSelected = true;
    }
    this.newFilterEvent.emit(this.filterEvent);
  };

  updateTranslations() {
    this.translateService.get(this.filterKey).subscribe((translation) => {
      this.filterLabel = translation;
    });
  }
}
