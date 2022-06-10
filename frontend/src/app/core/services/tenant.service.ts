import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable, Subject } from 'rxjs';
import { User } from '../../components/login/login.interfaces';

@Injectable({
  providedIn: 'root',
})
export class TenantService {
  private user = new Subject<User>();
  user$ = this.user.asObservable();

  private activeTenant = new BehaviorSubject('');
  activeTenant$ = this.activeTenant.asObservable();

  constructor() {
    let localStorageTenant = localStorage.getItem('tenant');
    if (localStorageTenant) {
      this.activeTenant.next(localStorageTenant);
    }
  }

  updateLoggedInUser(user: User) {
    this.user.next(user);
  }

  updateActiveTenant(tenant: string) {
    localStorage.setItem('tenant', tenant);
    this.activeTenant.next(tenant);
  }

  getTenant(): Observable<string> {
    return this.activeTenant$;
  }
}
