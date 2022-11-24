import { Observable, BehaviorSubject, filter, map } from 'rxjs';
import { Injectable } from '@angular/core';
import { MsalBroadcastService, MsalService } from '@azure/msal-angular';
import { AccountInfo, EventMessage, EventType, AuthenticationResult } from '@azure/msal-browser';

@Injectable()
export class AuthService {
  private user = new BehaviorSubject<User>(undefined);

  constructor(private msalBroadcastService: MsalBroadcastService, private msalService: MsalService) {
    this.user.next(this.toUser(this.msalService.instance.getAllAccounts()[0]));

    msalBroadcastService.msalSubject$
      .pipe(
        filter((msg: EventMessage) => [EventType.LOGIN_SUCCESS].includes(msg.eventType)),
        map((result: EventMessage) => {
          const payload = result.payload as AuthenticationResult;
          this.msalService.instance.setActiveAccount(payload.account);
          return payload.account;
        }),
        map(this.toUser)
      )
      .subscribe((user: User) => {
        this.user.next(user);
      });
  }

  private toUser(user: AccountInfo): User {
    if (!user) {
      return undefined;
    }

    return {
      name: user.name,
      email: user.username,
    };
  }

  onUserChanged(): Observable<User> {
    return this.user.asObservable();
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
