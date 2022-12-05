import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { TranslateModule } from '@ngx-translate/core';
import { RecordingDetailComponent } from './recording-detail.component';
import { HttpClientModule } from '@angular/common/http';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';

describe('RecordingDetailComponent', () => {
  let component: RecordingDetailComponent;
  let fixture: ComponentFixture<RecordingDetailComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RecordingDetailComponent],
      imports: [RouterTestingModule, TranslateModule.forRoot(), HttpClientModule, MatDialogModule],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            snapshot: {
              paramMap: {
                get: () => 'cae2bd19-38f1-4d9f-8b09-6632fa793c4d',
              },
            },
          },
        },
        {
          provide: MatDialogRef,
          useValue: {},
        },
        { provide: MAT_DIALOG_DATA, useValue: {} },
      ],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RecordingDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should get the video player width', () => {
    component.videoPlayerWidthPercentage = 50;
    expect(component.getVideoPlayerWidth()).toEqual('50%');
  });
});
