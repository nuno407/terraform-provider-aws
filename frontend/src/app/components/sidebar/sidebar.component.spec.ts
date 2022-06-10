import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { TranslateModule } from '@ngx-translate/core';
import { of } from 'rxjs';
import { Activity } from 'src/app/models/activity';
import { Label } from 'src/app/models/label';
import { LabelingService } from 'src/app/core/services/labeling.service';

import { SidebarComponent } from './sidebar.component';

describe('SidebarComponent', () => {
  let component: SidebarComponent;
  let fixture: ComponentFixture<SidebarComponent>;
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
      imports: [TranslateModule.forRoot()],
      declarations: [SidebarComponent],
      providers: [LabelingService],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SidebarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    service = TestBed.inject(LabelingService);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should set the frame', () => {
    component.frame = 20;
    expect(component.currentFrame).toEqual(20);
  });

  it('should set the frame and update the label', () => {
    spyOn(service, 'updateLabel').and.callThrough();
    component.selectedLabelIndex = 0;
    component.selectedFrame = 'start';
    component.labels = [label];
    component.frame = 20;

    expect(component.currentFrame).toEqual(20);
    expect(service.updateLabel).toHaveBeenCalled();
  });

  it('should set the selected label index', () => {
    spyOn(service, 'getSelectedLabelIndex').and.returnValue(of(1));
    component.ngOnInit();
    expect(component.selectedLabelIndex).toEqual(1);
  });

  it('should add a label', () => {
    spyOn(service, 'addLabel').and.callThrough();
    component.addLabel();
    expect(service.addLabel).toHaveBeenCalled();
  });

  it('should delete a label', () => {
    spyOn(service, 'deleteLabel').and.callThrough();
    component.onLabelDelete(0);
    expect(service.deleteLabel).toHaveBeenCalledWith(0);
  });

  it('should make a label editable', () => {
    spyOn(service, 'setSelectedLabelIndex').and.callThrough();
    component.onLabelEdit(0, true);
    expect(service.setSelectedLabelIndex).toHaveBeenCalledWith(0);
  });

  it('should make a label not editable', () => {
    spyOn(service, 'setSelectedLabelIndex').and.callThrough();
    component.onLabelEdit(0, false);
    expect(service.setSelectedLabelIndex).toHaveBeenCalledWith(-1);
  });

  it('should select a frame', () => {
    component.onFrameSelect('start');
    expect(component.selectedFrame).toEqual('start');
  });

  it('should toggle visibility of a label', () => {
    component.labels = [label];
    spyOn(service, 'updateLabel').and.callThrough();

    component.onVisibilityToggle(0, true);
    let labelNew = { ...label };
    labelNew.visibility = true;
    expect(service.updateLabel).toHaveBeenCalledWith(0, labelNew);

    component.onVisibilityToggle(0, false);
    labelNew = { ...label };
    labelNew.visibility = false;
    expect(service.updateLabel).toHaveBeenCalledWith(0, labelNew);
  });
});
