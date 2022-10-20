import { TestBed, waitForAsync } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { BciCoreModule, BciLayoutModule } from '@bci-web-core/core';

import { AppComponent } from './app.component';
import { TranslateService } from '@ngx-translate/core';
import { routes } from './app-routing.module';

describe('AppComponent', () => {
  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      imports: [RouterTestingModule.withRoutes(routes), NoopAnimationsModule, BciCoreModule.forRoot({ prod_environment: false }), BciLayoutModule],
      declarations: [AppComponent],
      providers: [{ provide: TranslateService, useValue: {} }],
    }).compileComponents();
  }));

  it('should create the app', () => {
    const fixture = TestBed.createComponent(AppComponent);
    const app = fixture.debugElement.componentInstance;
    expect(app).toBeTruthy();
  });
});
