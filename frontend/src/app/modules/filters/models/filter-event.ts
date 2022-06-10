import { FilterType } from './filter-type.enum';

export class FilterEvent<T> {
  columnName: string;
  filterType: FilterType;
  filterValue: T;
  isActive: boolean;
  isNotSelected: boolean;
}
