import { Component, OnInit } from '@angular/core';
import { FormControl, FormBuilder } from '@angular/forms';
import { MatDialogRef } from '@angular/material/dialog';
import { LanguageSelectorModel } from './model/language-selector-model';
import { TranslateService } from '@ngx-translate/core';

@Component({
  selector: 'app-language-selector',
  templateUrl: './language-selector.component.html',
  styleUrls: ['./language-selector.component.scss'],
})
export class LanguageSelectorComponent implements OnInit {
  /**Static array */
  languages: LanguageSelectorModel[] = [
    { id: 'en', name: 'English' },
    { id: 'de', name: 'Deutsch' },
  ];
  languagesControl: FormControl = this.fb.control({});

  constructor(public dialogRef: MatDialogRef<LanguageSelectorComponent>, private fb: FormBuilder, private translateService: TranslateService) {}

  /**Aplication state */
  ngOnInit() {
    const langId = localStorage.getItem('lang') ? localStorage.getItem('lang') : this.translateService.getBrowserLang();
    this.languagesControl.setValue(langId.slice(0, 2));
  }

  onLanguageChanged() {
    const langId = this.languagesControl.value;
    this.translateService.use(langId);
    this.dialogRef.close();
  }
}
