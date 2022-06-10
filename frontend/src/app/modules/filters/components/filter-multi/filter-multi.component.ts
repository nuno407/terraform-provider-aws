import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { FilterEvent } from '../../models/filter-event';
import { MultiselectFilterConfiguration } from '../../models/multiselect-filter-configuration';
import { FilterType } from '../../models/filter-type.enum';
import { TranslateService } from '@ngx-translate/core';

@Component({
  selector: 'app-filter-multi',
  templateUrl: './filter-multi.component.html',
  styleUrls: ['./filter-multi.component.scss'],
})
export class FilterMultiComponent implements OnInit {
  @Input()
  set configuration(config: MultiselectFilterConfiguration) {
    if (config) {
      this.options = config.options;
      this.checkedOptions = new Set<string>(this.options);
      this.filterEvent.columnName = config.columnName;
      this.filterEvent.filterType = FilterType.MultiselectFilter;
    }
  }

  @Output() newFilterEvent = new EventEmitter<FilterEvent<String[]>>();

  /**Local Variables */
  filterLabel = 'All';
  filterKey = 'FILTER.all';
  selectAllLabel = 'Unselect All';
  selectKey = 'FILTER.unselectAll';

  options = [];
  checkedOptions: Set<string> = new Set();

  filterEvent = new FilterEvent<string[]>();

  constructor(private translateService: TranslateService) {}

  /**Aplication state */
  ngOnInit(): void {
    this.updateTranslations();
    this.translateService.onLangChange.subscribe(() => {
      this.updateTranslations();
    });
  }

  selectAllClick() {
    if (this.checkedOptions.size < this.options.length) {
      this.options.forEach((option) => this.checkedOptions.add(option));
    } else {
      this.options.forEach((option) => this.checkedOptions.delete(option));
    }
    this.updateLabels();
  }

  checkboxClick(option: string) {
    if (this.checkedOptions.has(option)) {
      this.checkedOptions.delete(option);
    } else {
      this.checkedOptions.add(option);
    }
    this.updateLabels();
  }

  updateLabels() {
    this.updateFilterLabel();
    this.updateSelectAllLabel();

    this.filterEvent.filterValue = [...this.checkedOptions];
    this.filterEvent.isActive = true;
    this.newFilterEvent.emit(this.filterEvent);
  }

  updateFilterLabel() {
    const count = this.checkedOptions.size;
    this.filterKey = this.options.length === count ? 'FILTER.all' : 'FILTER.filterN';
    this.updateTranslations();
  }

  updateSelectAllLabel() {
    this.selectKey = this.checkedOptions.size < this.options.length ? 'FILTER.selectAll' : 'FILTER.unselectAll';
    this.updateTranslations();
  }

  updateTranslations() {
    this.translateService.get(this.filterKey, { n: this.checkedOptions.size }).subscribe((translation) => {
      this.filterLabel = translation;
    });
    this.translateService.get(this.selectKey).subscribe((translation) => {
      this.selectAllLabel = translation;
    });
  }
}
