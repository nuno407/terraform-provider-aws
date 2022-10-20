import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SignalSelectionComponent } from './signal-selection.component';

describe('SignalSelectionComponent', () => {
  let component: SignalSelectionComponent;
  let fixture: ComponentFixture<SignalSelectionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SignalSelectionComponent],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SignalSelectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
