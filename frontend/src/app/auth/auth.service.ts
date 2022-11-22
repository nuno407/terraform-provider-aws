import { Observable, BehaviorSubject, filter, map } from 'rxjs';
import { Injectable } from '@angular/core';
import { MsalBroadcastService, MsalService } from '@azure/msal-angular';
import { AccountInfo, EventMessage, EventType, AuthenticationResult } from '@azure/msal-browser';

@Injectable()
export class AuthService {
  private user = new BehaviorSubject<User>(undefined);
  user$ = this.user.asObservable();

  constructor(private msalBroadcastService: MsalBroadcastService, private msalService: MsalService) {
    this.user.next(this.toUser(this.msalService.instance.getAllAccounts()[0]));

    msalBroadcastService.msalSubject$
      .pipe(
        filter((msg: EventMessage) =>
          [EventType.LOGIN_SUCCESS, EventType.ACQUIRE_TOKEN_SUCCESS, EventType.SSO_SILENT_SUCCESS, EventType.ACQUIRE_TOKEN_BY_CODE_SUCCESS].includes(
            msg.eventType
          )
        ),
        map((msg: EventMessage) => (<AuthenticationResult>msg.payload).account), //NOSONAR
        map(this.toUser)
      )
      .subscribe(this.user.next);
  }

  private toUser(user: AccountInfo): User {
    if (!user) return undefined;
    return {
      name: user.name,
      email: user.username,
    };
  }

  isAuthenticated(): boolean {
    return !!this.user.value;
  }

  logout(): Observable<void> {
    this.user.next(undefined);
    return this.msalService.logout();
  }

  login(): Observable<void> {
    return this.msalService.loginRedirect();
  }
}

export class User {
  name: string;
  email: string;
}
