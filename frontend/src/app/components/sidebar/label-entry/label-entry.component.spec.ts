import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { LabelEntryComponent } from './label-entry.component';

describe('LabelEntryComponent', () => {
  let component: LabelEntryComponent;
  let fixture: ComponentFixture<LabelEntryComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [LabelEntryComponent],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LabelEntryComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should make a label editable', () => {
    spyOn(component.edit, 'emit');
    component.inEditMode = true;
    component._editLabel();
    expect(component.selectedFrame).toEqual(undefined);
    expect(component.edit.emit).toHaveBeenCalledWith(false);

    component.inEditMode = false;
    component._editLabel();
    expect(component.selectedFrame).toEqual(undefined);
    expect(component.edit.emit).toHaveBeenCalledWith(true);
  });

  it('should select a frame #1', () => {
    spyOn(component.selectFrame, 'emit');

    component.inEditMode = true;
    component.selectedFrame = 'start';
    component._selectFrame('start');
    expect(component.selectedFrame).toEqual(undefined);
    expect(component.selectFrame.emit).toHaveBeenCalledWith(undefined);
  });

  it('should select a frame #2', () => {
    spyOn(component.selectFrame, 'emit');

    component.inEditMode = true;
    component.selectedFrame = 'start';
    component._selectFrame('end');
    expect(component.selectedFrame).toEqual('end');
    expect(component.selectFrame.emit).toHaveBeenCalledWith('end');
  });

  it('should select a frame #3', () => {
    spyOn(component.selectFrame, 'emit');

    component.inEditMode = false;
    component._selectFrame('start');
    expect(component.selectFrame.emit).not.toHaveBeenCalled();
  });

  it('should delete a frame', () => {
    spyOn(component.delete, 'emit');

    component._delete();
    expect(component.delete.emit).toHaveBeenCalled();
  });

  it('should toggle the visibility', () => {
    spyOn(component.toggleVisibility, 'emit');

    component.visibility = true;
    component._toggleVisibility();
    expect(component.visibility).toBeFalse();
    expect(component.toggleVisibility.emit).toHaveBeenCalledWith(false);

    component.visibility = false;
    component._toggleVisibility();
    expect(component.visibility).toBeTrue();
    expect(component.toggleVisibility.emit).toHaveBeenCalledWith(true);
  });
});
