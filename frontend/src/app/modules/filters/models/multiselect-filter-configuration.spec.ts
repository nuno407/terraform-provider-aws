import { MultiselectFilterConfiguration } from './multiselect-filter-configuration';

describe('MultiselectFilterConfiguration', () => {
  it('should create an instance', () => {
    expect(new MultiselectFilterConfiguration('vin', [])).toBeTruthy();
  });
});
