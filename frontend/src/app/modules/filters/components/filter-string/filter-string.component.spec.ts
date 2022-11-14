import { ComponentFixture, TestBed, waitForAsync } from '@angular/core/testing';
import { FilterStringComponent } from './filter-string.component';
import { MatMenuModule } from '@angular/material/menu';
import { MatIconModule } from '@angular/material/icon';
import { MatRadioModule } from '@angular/material/radio';
import { StringFilterConfiguration } from '../../models/string-filter-configuration';
import { TranslateModule } from '@ngx-translate/core';

describe('FilterStringComponent', () => {
  let component: FilterStringComponent;
  let fixture: ComponentFixture<FilterStringComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [FilterStringComponent],
      imports: [MatMenuModule, MatIconModule, MatRadioModule, TranslateModule.forRoot()],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FilterStringComponent);
    component = fixture.componentInstance;
    component.configuration = new StringFilterConfiguration('vin', 'placeholder');
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
