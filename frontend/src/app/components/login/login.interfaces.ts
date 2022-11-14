export interface User {
  username: string;
  attributes: {
    email: string;
    sub: string;
    email_verified: boolean;
    'custom:localization'?: string;
  };
  signInUserSession: {
    accessToken: CognitoAccessToken;
    lockDrift: number;
    idToken: any;
    refreshToken: any;
  };
}

export interface CognitoAccessToken {
  jwtToken: string;
  payload: {
    client_id: string;
    'cognito:groups': string[];
    exp: number;
    iat: number;
    iss: string;
    jti: string;
    scope: string;
    sub: string;
    token_use: string;
    username: string;
    version: number;
  };
}
