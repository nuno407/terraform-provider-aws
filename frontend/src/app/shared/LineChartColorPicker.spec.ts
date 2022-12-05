import { LineChartColorPicker } from './LineChartColorPicker';

describe('FilterEvent', () => {
  it('should return first CHC color', () => {
    // GIVEN
    const colorPicker = new LineChartColorPicker();

    // WHEN
    const color = colorPicker.getNextChcColor();

    // THEN
    expect(color).toHaveSize(7);
    expect(color).toMatch('^#[a-f0-9]{6}$');
  });

  it('second CHC color should be different from first', () => {
    // GIVEN
    const colorPicker = new LineChartColorPicker();
    const color = colorPicker.getNextChcColor();

    // WHEN
    const secondColor = colorPicker.getNextChcColor();

    // THEN
    expect(secondColor).not.toEqual(color);
    expect(color).toMatch('^#[a-f0-9]{6}$');
  });

  it('should reset to first CHC color', () => {
    // GIVEN
    const colorPicker = new LineChartColorPicker();
    const color = colorPicker.getNextChcColor();
    colorPicker.getNextChcColor();

    // WHEN
    colorPicker.reset();
    const resetColor = colorPicker.getNextChcColor();

    // THEN
    expect(resetColor).toEqual(color);
  });

  it('should loop back to first CHC color', () => {
    // GIVEN
    const colorPicker = new LineChartColorPicker();
    const firstColor = colorPicker.getNextChcColor();
    let looped = false;
    for (let i = 0; i < 20; i++) {
      // WHEN
      const nextColor = colorPicker.getNextChcColor();

      // THEN
      if (firstColor === nextColor) {
        looped = true;
        break;
      }
    }

    expect(looped).toBeTruthy();
  });

  it('should return first MDF color', () => {
    // GIVEN
    const colorPicker = new LineChartColorPicker();

    // WHEN
    const color = colorPicker.getNextMdfColor();

    // THEN
    expect(color).toHaveSize(7);
    expect(color).toMatch('^#[a-f0-9]{6}$');
  });

  it('second MDF color should be different from first', () => {
    // GIVEN
    const colorPicker = new LineChartColorPicker();
    const color = colorPicker.getNextChcColor();

    // WHEN
    const secondColor = colorPicker.getNextMdfColor();

    // THEN
    expect(secondColor).not.toEqual(color);
    expect(color).toMatch('^#[a-f0-9]{6}$');
  });

  it('should reset to first MDF color', () => {
    // GIVEN
    const colorPicker = new LineChartColorPicker();
    const color = colorPicker.getNextMdfColor();
    colorPicker.getNextMdfColor();

    // WHEN
    colorPicker.reset();
    const resetColor = colorPicker.getNextMdfColor();

    // THEN
    expect(resetColor).toEqual(color);
  });

  it('should loop back to first MDF color', () => {
    // GIVEN
    const colorPicker = new LineChartColorPicker();
    const firstColor = colorPicker.getNextMdfColor();
    let looped = false;
    for (let i = 0; i < 20; i++) {
      // WHEN
      const nextColor = colorPicker.getNextMdfColor();

      // THEN
      if (firstColor === nextColor) {
        looped = true;
        break;
      }
    }

    expect(looped).toBeTruthy();
  });
});
