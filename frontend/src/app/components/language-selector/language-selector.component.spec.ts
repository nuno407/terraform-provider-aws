import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { TranslateModule, TranslateService } from '@ngx-translate/core';
import { LanguageSelectorComponent } from './language-selector.component';

describe('LanguageSelectorComponent', () => {
  let component: LanguageSelectorComponent;
  let fixture: ComponentFixture<LanguageSelectorComponent>;

  const dialogMock = {
    close: () => {},
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [NoopAnimationsModule, ReactiveFormsModule, MatDialogModule, MatSelectModule, MatInputModule, MatFormFieldModule, TranslateModule.forRoot()],
      declarations: [LanguageSelectorComponent],
      providers: [{ provide: MatDialogRef, useValue: dialogMock }],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LanguageSelectorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should change the language', () => {
    const translateService = fixture.debugElement.injector.get(TranslateService);
    const translateSpy = spyOn(translateService, 'use').and.callThrough();
    const dialogSpy = spyOn(component.dialogRef, 'close').and.callThrough();
    component.languagesControl.setValue('de');
    component.onLanguageChanged();
    expect(dialogSpy).toHaveBeenCalled();
    expect(translateSpy).toHaveBeenCalledWith('de');
  });
});
