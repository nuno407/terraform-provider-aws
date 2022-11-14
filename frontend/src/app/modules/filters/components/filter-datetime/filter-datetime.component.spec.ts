import { ComponentFixture, TestBed, waitForAsync } from '@angular/core/testing';
import { TranslateModule } from '@ngx-translate/core';
import { MatMenuModule } from '@angular/material/menu';
import { MatIconModule } from '@angular/material/icon';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { StringFilterConfiguration } from '../../models/string-filter-configuration';
import { FilterDatetimeComponent } from './filter-datetime.component';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';

describe('FilterDatetimeComponent', () => {
  let component: FilterDatetimeComponent;
  let fixture: ComponentFixture<FilterDatetimeComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [FilterDatetimeComponent],
      imports: [MatMenuModule, MatIconModule, MatCheckboxModule, TranslateModule.forRoot()],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FilterDatetimeComponent);
    component = fixture.componentInstance;
    component.configuration = new StringFilterConfiguration('vin', 'placeholder');
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should return a correct timestamp', () => {
    const time = component.getTimestampFromString('11.05.2021', '02:00:00');
    const resultTime = new Date('2021-05-11T02:00:00').getTime();
    expect(time).toEqual(resultTime);
  });

  it('should activate the filter', () => {
    const event = {
      detail: { dateStart: '11.05.2021', dateEnd: '11.05.2021', timeStart: '01:00:00', timeEnd: '02:00:00', valid: true },
    };
    component.dateSelected(event);

    const resultStartTime = new Date('2021-05-11T01:00:00').getTime();
    const resultEndTime = new Date('2021-05-11T02:00:00').getTime();
    expect(component.filterEvent.isActive).toBeTrue();
    expect(component.filterEvent.filterValue).toEqual({
      startTime: resultStartTime,
      endTime: resultEndTime,
    });
  });

  it('should not activate the filter', () => {
    const event = {
      detail: {},
    };
    component.dateSelected(event);
    expect(component.filterEvent.isActive).toBeFalse();
  });
});
