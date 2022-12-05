import { FilterItemDirective } from './filter-item.directive';

describe('FilterItemDirective', () => {
  it('should create an instance', () => {
    const directive = new FilterItemDirective();
    expect(directive).toBeTruthy();
  });

  it('should stop propagation on click', () => {
    // GIVEN
    const directive = new FilterItemDirective();
    const mouseEvent = jasmine.createSpyObj(MouseEvent, ['stopPropagation', 'preventDefault']);

    // WHEN
    const clickReturn = directive.onClick(mouseEvent);

    // THEN
    expect(clickReturn).toBeFalsy();
    expect(mouseEvent.stopPropagation).toHaveBeenCalled();
    expect(mouseEvent.preventDefault).toHaveBeenCalled();
  });
});
