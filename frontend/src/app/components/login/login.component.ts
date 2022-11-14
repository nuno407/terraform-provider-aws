import { Component, NgZone, OnInit } from '@angular/core';
import { TranslateService } from '@ngx-translate/core';
import { Auth } from 'aws-amplify';
import { environment } from 'src/environments/environment';
import { AuthState, onAuthUIStateChange } from '@aws-amplify/ui-components';
import { Router } from '@angular/router';

@Component({
  selector: 'app-login',
  templateUrl: 'login.component.html',
  styleUrls: ['login.component.scss'],
})
export class LoginComponent implements OnInit {
  /**Local variables */
  title: 'RideCare Dashboard';
  formFields;

  constructor(public translate: TranslateService, private router: Router, private zone: NgZone) {
    this.formFields = [{ type: 'email' }, { type: 'password' }];
    const browserLang = translate.getBrowserLang();

    translate.use(browserLang.match(/en|de/) ? browserLang : 'en');
  }

  /**Aplication state */
  ngOnInit() {
    onAuthUIStateChange((nextAuthState) => {
      if (nextAuthState === AuthState.SignedIn) {
        //ng zone is required for components to render after calling from outside JS
        this.zone.run(() => {
          this.router.navigate(['/recording-overview']);
          console.log(nextAuthState);
          console.log(this.zone);
        });
      }
    });
  }

  onLoginClickAzureAD() {
    Auth.federatedSignIn({
      customProvider: environment.identityProvider,
    });
  }

  downloadLicenseDisclosureDocument() {
    window.open('/assets/files/LicenseDisclosureDocument.pdf', '_blank');
  }

  fixLogin() {
    localStorage.clear();
    sessionStorage.clear();
    this.windowReload();
  }

  windowReload() {
    window.location.reload();
  }
}
