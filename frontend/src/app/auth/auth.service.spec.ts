import { AuthService } from './auth.service';
import createSpyObj = jasmine.createSpyObj;
import { MsalBroadcastService, MsalService } from '@azure/msal-angular';
import { of } from 'rxjs';
import { EventType } from '@azure/msal-browser';

describe('AuthService', () => {
  let broadcastService;
  let msalService;
  let authService;
  let user;
  beforeEach(() => {
    user = {
      name: 'bumlux',
      username: 'bumlux@bosch.com',
    };
    broadcastService = createSpyObj(MsalBroadcastService, ['msalSubject$']);
    broadcastService.msalSubject$ = of({ eventType: EventType.LOGIN_SUCCESS, payload: { account: user } });
    msalService = createSpyObj(MsalService, ['instance', 'logout', 'loginRedirect']);
    msalService.instance = createSpyObj(['setActiveAccount', 'getAllAccounts']);
    msalService.instance.getAllAccounts.and.returnValue([user]);
  });

  it('should create an AuthService', (done) => {
    authService = new AuthService(broadcastService, msalService);
    expect(authService);
    expect(msalService.instance.setActiveAccount).toHaveBeenCalledWith(user);
    authService.onUserChanged().subscribe((value) => {
      expect(value).toEqual({ name: user.name, email: user.username });
      done();
    });
  });

  it('should return undefined on failed login', (done) => {
    broadcastService.msalSubject$ = of({ eventType: EventType.LOGIN_FAILURE, payload: { account: user } });
    msalService.instance.getAllAccounts.and.returnValue([undefined]);
    authService = new AuthService(broadcastService, msalService);
    authService.onUserChanged().subscribe((value) => {
      expect(value).toEqual(undefined);
      done();
    });
  });

  it('should return undefined on missing AccountInfo', (done) => {
    broadcastService.msalSubject$ = of({ eventType: EventType.LOGIN_SUCCESS, payload: { account: undefined } });
    msalService.instance.getAllAccounts.and.returnValue([undefined]);
    authService = new AuthService(broadcastService, msalService);
    authService.onUserChanged().subscribe((value) => {
      expect(value).toEqual(undefined);
      done();
    });
  });

  it('should set undefined user on logout', (done) => {
    authService = new AuthService(broadcastService, msalService);
    authService.logout();
    authService.onUserChanged().subscribe((value) => {
      expect(value).toEqual(undefined);
      done();
    });
  });

  it('should redirect on login', () => {
    authService = new AuthService(broadcastService, msalService);
    authService.login();

    expect(msalService.loginRedirect).toHaveBeenCalled();
  });
});
