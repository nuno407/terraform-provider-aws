import { HttpBackend, HttpClientModule } from '@angular/common/http';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { MatMenuModule } from '@angular/material/menu';
import { RouterTestingModule } from '@angular/router/testing';
import { BciCoreModule, BciLayoutModule } from '@bci-web-core/core';
import { TranslateLoader, TranslateModule, TranslateService } from '@ngx-translate/core';
import { HttpLoaderFactory } from '../app.module';
import { AdminComponent } from './admin.component';
import { of } from 'rxjs';
import Auth from '@aws-amplify/auth';
import { routes } from '../app-routing.module';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

describe('AdminComponent', () => {
  let component: AdminComponent;
  let fixture: ComponentFixture<AdminComponent>;

  beforeEach(() => {
    const user = { attributes: { email: 'user@de.bosch.com' } };
    Auth.currentUserPoolUser = jasmine.createSpy().and.returnValue(Promise.resolve(user));
    Auth.signOut = jasmine.createSpy().and.callFake(() => Promise.resolve());
  });

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        NoopAnimationsModule,
        HttpClientModule,
        TranslateModule.forRoot({
          loader: {
            provide: TranslateLoader,
            useFactory: HttpLoaderFactory,
            deps: [HttpBackend],
          },
        }),
        BciCoreModule.forRoot({
          prod_environment: false,
        }),
        BciLayoutModule,
        MatMenuModule,
        RouterTestingModule.withRoutes(routes),
      ],
      declarations: [AdminComponent],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should update the language', () => {
    const translateService = fixture.debugElement.injector.get(TranslateService);
    spyOnProperty(translateService, 'onLangChange', 'get').and.returnValue(of({ lang: 'en' }));
    spyOn(localStorage, 'setItem');
    component.ngOnInit();
    expect(localStorage.setItem).toHaveBeenCalledWith('lang', 'en');
  });

  it('should set the users email', async () => {
    const user = { attributes: { email: 'user@de.bosch.com' } };
    await fixture.whenStable();
    expect(component.email).toEqual(user.attributes.email);
  });

  it('should logout the user', async () => {
    await component.onLogoutClick();
    expect(Auth.signOut).toHaveBeenCalledTimes(1);
  });
});
