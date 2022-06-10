import Auth from '@aws-amplify/auth';
import { AuthGuardService } from './authguard.service';

class MockRouter {
  navigate(path) {}
}

describe('AuthGuardService', () => {
  let authGuard: AuthGuardService;
  let router;

  it('should return true for a logged in user', async () => {
    Auth.currentAuthenticatedUser = jasmine.createSpy().and.returnValue(Promise.resolve());
    router = new MockRouter();
    authGuard = new AuthGuardService(router);

    await expectAsync(authGuard.canActivate(null, null)).toBeResolved(true);
  });

  it('should navigate to login for a logged out user', async () => {
    Auth.currentAuthenticatedUser = jasmine.createSpy().and.returnValue(Promise.reject(''));
    router = new MockRouter();
    authGuard = new AuthGuardService(router);
    spyOn(router, 'navigate');

    await expectAsync(authGuard.canActivate(null, null)).toBeResolved(false);
    expect(router.navigate).toHaveBeenCalledWith(['login']);
  });
});
