export class StringFilterConfiguration {
  columnName: string;
  placeholderText?: string;

  constructor(columnName: string, placeholderText?: string) {
    this.columnName = columnName;
    if (placeholderText) {
      this.placeholderText = placeholderText;
    }
  }
}
