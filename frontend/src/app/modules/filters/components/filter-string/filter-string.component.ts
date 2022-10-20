import { Component, OnInit } from '@angular/core';
import { FilterType } from '../../models/filter-type.enum';
import { TranslateService } from '@ngx-translate/core';
import { FilterBaseComponent } from '../filter-base/filter-base.component';

@Component({
  selector: 'app-filter-string',
  templateUrl: './filter-string.component.html',
  styleUrls: ['./filter-string.component.scss'],
})
export class FilterStringComponent extends FilterBaseComponent implements OnInit {
  /**Local variables */
  placeholderText = '';

  constructor(translateService: TranslateService) {
    super(translateService);
  }

  /**Aplication state */
  ngOnInit(): void {
    this.filterEvent.columnName = this.configuration.columnName;
    this.filterEvent.filterType = FilterType.StringFilter;
    this.filterEvent.isActive = false;
    this.placeholderText = `${this.translateService.instant('FILTER.eg')} ${this.configuration.placeholderText}`;

    this.updateTranslations();
    this.translateService.onLangChange.subscribe(() => {
      this.updateTranslations();
    });
  }

  onKey(event: any) {
    this.filterEvent.filterValue = event.target.value;
    this.newFilterEvent.emit(this.filterEvent);
  }

  updateTranslations() {
    super.updateTranslations();
    this.placeholderText = `${this.translateService.instant('FILTER.eg')} ${this.configuration.placeholderText}`;
  }
}
