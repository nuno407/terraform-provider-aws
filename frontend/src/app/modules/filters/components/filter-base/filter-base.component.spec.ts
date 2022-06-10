import { ComponentFixture, TestBed } from '@angular/core/testing';
import { TranslateModule } from '@ngx-translate/core';
import { FilterBaseComponent } from './filter-base.component';

class TestFilter extends FilterBaseComponent {}

describe('FilterBaseComponent', () => {
  let component: TestFilter;
  let fixture: ComponentFixture<TestFilter>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [FilterBaseComponent],
      imports: [TranslateModule.forRoot()],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TestFilter);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should check radioClick: radio-contains', () => {
    component.radioClick(component.radioContainsId);
    expect(component.filterEvent.isNotSelected).toBeFalse();
    expect(component.filterEvent.isActive).toBeTrue();
  });

  it('should check radioClick: radio-all', () => {
    component.radioClick(component.radioAllId);
    expect(component.filterEvent.isNotSelected).toBeFalse();
    expect(component.filterEvent.isActive).toBeFalse();
  });

  it('should check radioClick: radio-not-selected', () => {
    component.radioClick(component.radioNotSelectedId);
    expect(component.filterEvent.isNotSelected).toBeTrue();
    expect(component.filterEvent.isActive).toBeTrue();
  });
});
