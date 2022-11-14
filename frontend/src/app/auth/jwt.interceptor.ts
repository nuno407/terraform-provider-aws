import { Injectable } from '@angular/core';
import { HttpEvent, HttpHandler, HttpInterceptor, HttpRequest } from '@angular/common/http';
import { from, Observable, throwError } from 'rxjs';
import { catchError, switchMap } from 'rxjs/operators';
import { Auth } from 'aws-amplify';

@Injectable()
export class JwtInterceptor implements HttpInterceptor {
  intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    return from(Auth.currentSession()).pipe(
      switchMap((auth: any) => {
        // switchMap() is used instead of map().
        let jwt = auth.accessToken.jwtToken;
        let with_auth_request = request.clone({
          setHeaders: {
            Authorization: `Bearer ${jwt}`,
          },
        });

        return next.handle(with_auth_request);
      }),
      catchError((err) => {
        const errToCatch = new Error(err);
        return throwError(() => errToCatch);
      })
    );
  }
}
