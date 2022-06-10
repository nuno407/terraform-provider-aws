import { StringFilterConfiguration } from './string-filter-configuration';

describe('StringFilterConfiguration', () => {
  it('should create an instance', () => {
    expect(new StringFilterConfiguration('col1', 'test')).toBeTruthy();
  });
});
