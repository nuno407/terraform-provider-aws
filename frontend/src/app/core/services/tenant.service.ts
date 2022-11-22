import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable, Subject } from 'rxjs';

@Injectable({
  providedIn: 'root',
})
export class TenantService {
  private activeTenant = new BehaviorSubject('');
  activeTenant$ = this.activeTenant.asObservable();

  constructor() {
    let localStorageTenant = localStorage.getItem('tenant');
    if (localStorageTenant) {
      this.activeTenant.next(localStorageTenant);
    }
  }

  updateActiveTenant(tenant: string) {
    localStorage.setItem('tenant', tenant);
    this.activeTenant.next(tenant);
  }

  getTenant(): Observable<string> {
    return this.activeTenant$;
  }
}
