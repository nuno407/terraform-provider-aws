import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { TranslateModule } from '@ngx-translate/core';
import { Subscription } from 'rxjs';
import { Activity } from 'src/app/models/activity';
import { Label } from 'src/app/models/label';
import { NoCommaPipe } from 'src/app/pipes/no-comma.pipe';

import { VideoPlayerComponent } from './video-player.component';
import { RouterTestingModule } from '@angular/router/testing';
import { HttpClientModule } from '@angular/common/http';
import { routes } from '../../app-routing.module';
import { MatSnackBar } from '@angular/material/snack-bar';
import { Overlay } from '@angular/cdk/overlay';

describe('VideoPlayerComponent', () => {
  let component: VideoPlayerComponent;
  let fixture: ComponentFixture<VideoPlayerComponent>;

  const drawLabel = { start: 0, end: 20, width: 20, visibility: true };
  const label: Label = {
    start: {
      frame: 0,
      seconds: 0,
    },
    end: {
      frame: 20,
      seconds: 20,
    },
    activities: new Activity(),
    visibility: true,
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TranslateModule.forRoot(), RouterTestingModule.withRoutes(routes), HttpClientModule],
      providers: [MatSnackBar, Overlay],
      declarations: [VideoPlayerComponent, NoCommaPipe],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(VideoPlayerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should get a label style', () => {
    expect(component.getLabelStyle(drawLabel)).toEqual({ width: drawLabel.width + '%', left: drawLabel.start + '%' });
  });

  it('should draw labels', () => {
    spyOnProperty(component.video, 'duration', 'get').and.returnValue(100);
    // @ts-ignore
    component.drawLabels([label]);
    expect(component.labels).toEqual([drawLabel]);
  });

  it('should stop reverse', () => {
    component.reverseSubscription = new Subscription();
    spyOn(component.reverseSubscription, 'unsubscribe').and.callThrough();
    // @ts-ignore
    component.stopReverse();
    expect(component.reverseSubscription.unsubscribe).toHaveBeenCalled();
  });

  it('should stop reverse', () => {
    component.reverseSubscription = new Subscription();
    component.reverseSubscription.unsubscribe();
    spyOn(component.reverseSubscription, 'unsubscribe').and.callThrough();
    // @ts-ignore
    component.stopReverse();
    expect(component.reverseSubscription.unsubscribe).not.toHaveBeenCalled();
  });

  it('should play forward', () => {
    // @ts-ignore
    spyOn(component, 'stopReverse').and.callThrough();
    spyOn(component, 'togglePlay').and.callThrough();
    component.forward();
    // @ts-ignore
    expect(component.stopReverse).toHaveBeenCalled();
    expect(component.togglePlay).toHaveBeenCalled();
  });

  it('should destroy the component', () => {
    component.keyboardSubscription = new Subscription();
    spyOn(component.keyboardSubscription, 'unsubscribe').and.callThrough();
    component.ngOnDestroy();
    expect(component.keyboardSubscription.unsubscribe).toHaveBeenCalled();
  });

  it('should update play header position', () => {
    component.updatePlayheaderPosition();
    expect(component.playheaderHidden).toBeFalsy();
  });
});
