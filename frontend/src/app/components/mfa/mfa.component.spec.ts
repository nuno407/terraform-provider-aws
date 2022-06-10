import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import Auth from '@aws-amplify/auth';
import { QRCodeModule } from 'angularx-qrcode';
import { MfaComponent } from './mfa.component';

describe('MfaComponent', () => {
  let component: MfaComponent;
  let fixture: ComponentFixture<MfaComponent>;

  beforeEach(() => {
    Auth.currentAuthenticatedUser = jasmine.createSpy().and.returnValue(Promise.resolve({ username: 'bosch' }));
    Auth.getPreferredMFA = jasmine.createSpy().and.returnValue(Promise.resolve('NOMFA'));
    Auth.setupTOTP = jasmine.createSpy().and.returnValue(Promise.resolve('123'));
    Auth.verifyTotpToken = jasmine.createSpy().and.callFake(() => Promise.resolve());
    Auth.setPreferredMFA = jasmine.createSpy().and.callFake(() => Promise.resolve());
  });

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [NoopAnimationsModule, FormsModule, MatFormFieldModule, MatInputModule, QRCodeModule],
      declarations: [MfaComponent],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MfaComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should test ngOnInit', async () => {
    await fixture.whenStable();
    expect(component.preferredSignInOption).toEqual('NOMFA');
    expect(component.code).toEqual('123');
    expect(component.qrCodeString).toEqual('otpauth://totp/AWSCognito:bosch?secret=123&issuer=' + 'RideCare');
  });

  it('should verify the challenge', async () => {
    component.challenge = '123';
    await component.verifyChallenge();
    await fixture.whenStable();
    expect(component.preferredSignInOption).toEqual('SOFTWARE_TOKEN_MFA');
  });

  it('should fail the verification of the challenge', () => {
    component.challenge = '';
    component.verifyChallenge();
    expect(component.preferredSignInOption).toEqual('NOMFA');
  });

  it('should fail the verification of the token', () => {
    Auth.verifyTotpToken = jasmine.createSpy().and.callFake(() => Promise.reject());
    component.challenge = '123';
    component.verifyChallenge();
    expect(component.preferredSignInOption).toEqual('NOMFA');
  });
});
