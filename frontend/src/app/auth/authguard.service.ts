import { Injectable } from '@angular/core';
import { UrlTree } from '@angular/router';
import { Router, CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot } from '@angular/router';
import { MsalGuard } from '@azure/msal-angular';
import { Observable } from 'rxjs';
import { AuthService } from './auth.service';

/**
 * Prevent access to routes if access-token is not present.
 *
 * @export
 * @class AuthGuard
 * @implements {CanActivate}
 */
@Injectable()
export class AuthGuardService implements CanActivate {
  constructor(private _router: Router, private authService: AuthService, private msalGuard: MsalGuard) {}

  canActivate(next: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<boolean | UrlTree> | boolean {
    return this.authService.isAuthenticated() || this.msalGuard.canActivate(next, state);
  }
}
