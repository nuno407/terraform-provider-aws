import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';
import { Activity } from 'src/app/models/activity';
import { Label } from 'src/app/models/label';
import { LabelingService } from 'src/app/core/services/labeling.service';

import { ActivitiesComponent } from './activities.component';

describe('ActivitiesComponent', () => {
  let component: ActivitiesComponent;
  let fixture: ComponentFixture<ActivitiesComponent>;
  let service: LabelingService;

  const label: Label = {
    start: {
      frame: 0,
      seconds: 0,
    },
    end: {
      frame: 20,
      seconds: 1,
    },
    activities: new Activity(),
    visibility: true,
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ActivitiesComponent],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ActivitiesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    service = TestBed.inject(LabelingService);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should return a negative selected label index', () => {
    spyOn(service, 'getSelectedLabelIndex').and.returnValue(of(-1));
    component.ngOnInit();
    expect(component.selectedLabelIndex).toEqual(null);
    expect(component.activities).toEqual(new Activity());
  });

  it('should return a valid selected label index', () => {
    spyOn(service, 'getSelectedLabelIndex').and.returnValue(of(0));
    spyOn(service, 'getLabels').and.returnValue(of([label]));
    component.ngOnInit();
    expect(component.selectedLabelIndex).toEqual(0);
    expect(component.activities).toEqual(label.activities);
  });

  it('should update radio button activity', () => {
    spyOn(service, 'updateLabel');
    component.labels = [label];
    component.selectedLabelIndex = 0;

    component.radioChange('nonPhysicalAggression', 1);

    const result = label;
    result.activities.nonPhysicalAggression.selected = 1;

    expect(service.updateLabel).toHaveBeenCalledWith(0, result);
  });

  it('should change the weapon value', () => {
    spyOn(service, 'updateLabel');
    component.labels = [label];
    component.selectedLabelIndex = 0;
    const event = { checked: true };

    component.toggleChange('physicalAggression.weapon', event);

    const result = label;
    result.activities.physicalAggression.weapon.value = true;

    expect(service.updateLabel).toHaveBeenCalledWith(0, result);
  });

  it('should change other toggle values', () => {
    spyOn(service, 'updateLabel');
    component.labels = [label];
    component.selectedLabelIndex = 0;
    const event = { checked: true };

    component.toggleChange('driving', event);

    const result = label;
    result.activities.driving = true;

    expect(service.updateLabel).toHaveBeenCalledWith(0, result);
  });

  it('should change the number of occupants #1', () => {
    spyOn(service, 'updateLabel');
    component.labels = [label];
    component.selectedLabelIndex = 0;
    component.changeOccupants(4);

    const result = label;
    result.activities.occupants = 4;

    expect(service.updateLabel).toHaveBeenCalledWith(0, result);
  });

  it('should change the number of occupants #2', () => {
    spyOn(service, 'updateLabel');
    component.labels = [label];
    component.selectedLabelIndex = 0;
    component.changeOccupants(-2);

    const result = label;
    result.activities.occupants = 0;

    expect(service.updateLabel).toHaveBeenCalledWith(0, result);
  });
});
