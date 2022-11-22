import { BehaviorSubject } from 'rxjs';
import { Injectable } from '@angular/core';

@Injectable()
export class AuthService {
    private _user = new BehaviorSubject<User>(undefined);
    user$ = this._user.asObservable();

    isAuthenticated(): boolean {
        return !!this._user.value;
    }

    logout(): Promise<void> {
        this._user.next(undefined);
        return Promise.resolve();
    }
}

export class User {
    id: string;
    email: string;
}
