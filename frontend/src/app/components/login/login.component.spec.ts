import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { TranslateModule } from '@ngx-translate/core';
import { LoginComponent } from './login.component';

describe('LoginComponent', () => {
  let component: LoginComponent;
  let fixture: ComponentFixture<LoginComponent>;

  let store = {};
  let session = {};

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RouterTestingModule, TranslateModule.forRoot()],
      declarations: [LoginComponent],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LoginComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();

    store = {};
    spyOn(localStorage, 'clear').and.callFake(() => {
      store = {};
    });
    session = {};
    spyOn(sessionStorage, 'clear').and.callFake(() => {
      session = {};
    });
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should fix the login', () => {
    spyOn(component, 'windowReload').and.callFake(() => {});

    component.fixLogin();
    expect(store).toEqual({});
    expect(session).toEqual({});
  });
});
