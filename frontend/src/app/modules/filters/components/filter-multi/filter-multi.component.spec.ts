import { ComponentFixture, TestBed, waitForAsync } from '@angular/core/testing';
import { FilterMultiComponent } from './filter-multi.component';
import { MatMenuModule } from '@angular/material/menu';
import { MatIconModule } from '@angular/material/icon';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { TranslateModule } from '@ngx-translate/core';
import { MultiselectFilterConfiguration } from '../../models/multiselect-filter-configuration';
import { FilterType } from '../../models/filter-type.enum';

describe('FilterMultiComponent', () => {
  let component: FilterMultiComponent;
  let fixture: ComponentFixture<FilterMultiComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [FilterMultiComponent],
      imports: [MatMenuModule, MatIconModule, MatCheckboxModule, TranslateModule.forRoot()],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FilterMultiComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should mark a checkbox as checked', () => {
    component.checkboxClick('Video');
    expect(component.checkedOptions.has('Video')).toBeTrue();
  });

  it('should mark a checkbox as unchecked', () => {
    component.checkboxClick('Video');
    component.checkboxClick('Video');
    expect(component.checkedOptions.has('Video')).toBeFalse();
  });

  it('should mark all options as checked', () => {
    component.options = ['Video', 'Audio', 'Cars'];
    component.selectAllClick();
    expect(Array.from(component.checkedOptions)).toEqual(['Video', 'Audio', 'Cars']);
  });

  it('should mark all options as unchecked', () => {
    component.checkedOptions.add('Video');
    component.checkedOptions.add('Audio');
    component.checkedOptions.add('Cars');
    component.options = ['Video', 'Audio', 'Cars'];
    component.selectAllClick();
    expect(component.checkedOptions.size).toEqual(0);
  });

  it('should test the input configuration', () => {
    const configuration = new MultiselectFilterConfiguration('id', ['undefined', '123']);
    component.configuration = configuration;
    expect(component.options).toEqual(configuration.options);
    expect(component.checkedOptions).toEqual(new Set<string>(configuration.options));
    expect(component.filterEvent.columnName).toEqual(configuration.columnName);
    expect(component.filterEvent.filterType).toEqual(FilterType.MultiselectFilter);
  });
});
