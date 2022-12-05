import { HttpBackend, HttpClientModule } from '@angular/common/http';
import { CUSTOM_ELEMENTS_SCHEMA, EventEmitter } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { MatMenuModule } from '@angular/material/menu';
import { RouterTestingModule } from '@angular/router/testing';
import { BciCoreModule, BciLayoutModule } from '@bci-web-core/core';
import { LangChangeEvent, TranslateLoader, TranslateModule, TranslateService } from '@ngx-translate/core';
import { HttpLoaderFactory } from '../app.module';
import { AdminComponent } from './admin.component';
import { of } from 'rxjs';
import { routes } from '../app-routing.module';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { AuthService } from '../auth/auth.service';

describe('AdminComponent', () => {
  let component: AdminComponent;
  let fixture: ComponentFixture<AdminComponent>;
  let mockedAuthService;

  beforeEach(() => {
    const user = {
      name: 'Bumlux',
      email: 'bumlux@bosch.com',
    };
    mockedAuthService = jasmine.createSpyObj(AdminComponent, ['onUserChanged', 'login', 'logout']);
    mockedAuthService.onUserChanged.and.returnValue(of(user));
    mockedAuthService.logout.and.returnValue(of(null));
  });

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [{ provide: AuthService, useValue: mockedAuthService }],
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
    let eventEmitter = new EventEmitter<LangChangeEvent>();
    spyOnProperty(translateService, 'onLangChange', 'get').and.returnValue(eventEmitter);
    spyOn(localStorage, 'setItem');
    component.ngOnInit();

    eventEmitter.emit({ lang: 'en', translations: [] });

    expect(localStorage.setItem).toHaveBeenCalledWith('lang', 'en');
  });

  it('should set the users email', async () => {
    await fixture.whenStable();
    expect(component.username).toEqual('Bumlux');
  });

  it('should logout the user', async () => {
    await fixture.whenStable();
    component.onLogoutClick();
    expect(component.username).toBeFalsy();
  });
});
