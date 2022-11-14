import { Component, OnInit } from '@angular/core';
import Auth from '@aws-amplify/auth';

@Component({
  selector: 'mfa',
  templateUrl: './mfa.component.html',
  styleUrls: ['./mfa.component.scss'],
})
export class MfaComponent implements OnInit {
  /**Local variables */
  preferredSignInOption = 'NOMFA';
  qrCodeString = '';
  challenge = '';
  code = '';

  constructor() {}

  /**Aplication state */
  ngOnInit(): void {
    Auth.currentAuthenticatedUser().then((user) => {
      console.log(user);
      Auth.getPreferredMFA(user).then((preferred) => {
        this.preferredSignInOption = preferred;
        if (preferred == 'NOMFA') {
          Auth.setupTOTP(user).then((code) => {
            this.code = code;
            this.qrCodeString = 'otpauth://totp/AWSCognito:' + user.username + '?secret=' + code + '&issuer=' + 'RideCare';
          });
        }
      });
    });
  }

  verifyChallenge() {
    Auth.currentAuthenticatedUser().then((user) => {
      console.log(this.challenge);
      if (this.challenge.length > 0) {
        Auth.verifyTotpToken(user, this.challenge)
          .then(() => {
            Auth.setPreferredMFA(user, 'TOTP');
            this.preferredSignInOption = 'SOFTWARE_TOKEN_MFA';
          })
          .catch((e) => {
            console.log('Error verifying Challenge');
            // Token is not verified
          });
      } else {
        console.log('No Challenge');
      }
    });
  }
}
