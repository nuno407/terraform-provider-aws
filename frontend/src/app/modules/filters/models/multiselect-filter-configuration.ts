export class MultiselectFilterConfiguration {
  columnName: string;
  options: string[];

  constructor(columnName: string, options: string[]) {
    this.columnName = columnName;
    this.options = options;
  }
}
