export class LineChartColorPicker {
  protected CHC = ['#c24747', '#993333', '#cf746e', '#de9f9c', '#5c2d1f', '#bb3e66', '#bf40a8', '#c7a257', '#b4743c', '#dd98c0', '#a49f37'];
  protected MDF = ['#4770c2', '#2b6282', '#29417a', '#2b8182', '#2e8a60', '#53c6ad', '#8cd9cf', '#8cd9aa', '#30913a', '#4c44c1', '#57c787'];

  protected mdfIndex = 0;
  protected chcIndex = 0;

  reset() {
    this.mdfIndex = 0;
    this.chcIndex = 0;
  }

  getNextMdfColor(): string {
    let color = this.MDF[this.mdfIndex];
    this.mdfIndex++;
    if (this.mdfIndex >= this.MDF.length) this.mdfIndex = 0;
    return color;
  }

  getNextChcColor(): string {
    let color = this.CHC[this.chcIndex];
    this.chcIndex++;
    if (this.chcIndex >= this.CHC.length) this.chcIndex = 0;
    return color;
  }
}
