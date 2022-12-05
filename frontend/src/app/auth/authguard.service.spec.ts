import { AuthGuardService } from './authguard.service';
import { AuthService } from './auth.service';
import { MsalGuard } from '@azure/msal-angular';

class MockRouter {
  navigate(path) {}
}

describe('AuthGuardService', () => {
  let router;
  let service;
  let msal;

  it('should return true for a logged in user', async () => {
    router = new MockRouter();
    service = jasmine.createSpyObj(AuthService, ['login']);
    msal = jasmine.createSpyObj(MsalGuard, ['canActivate']);
    msal.canActivate.and.returnValue(true);
    let authGuard = new AuthGuardService(router, service, msal);

    expect(authGuard.canActivate(null, null)).toBeTruthy();
  });

  it('should navigate to login for a logged out user', async () => {
    router = new MockRouter();
    service = jasmine.createSpyObj(AuthService, ['login']);
    msal = jasmine.createSpyObj(MsalGuard, ['canActivate']);
    msal.canActivate.and.returnValue(false);
    let authGuard = new AuthGuardService(router, service, msal);
    spyOn(router, 'navigate');

    expect(authGuard.canActivate(null, null)).toBeFalsy();
  });
});
