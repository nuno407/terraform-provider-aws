/* eslint-disable */
// WARNING: DO NOT EDIT. This file is automatically generated by AWS Amplify. It will be overwritten.

const awsmobile = {
  aws_project_region: 'eu-central-1',
  aws_cognito_identity_pool_id: 'eu-central-1:21068fdd-2d6f-4b30-809b-91f0cae03503',
  aws_cognito_region: 'eu-central-1',
  aws_user_pools_id: 'eu-central-1_Oc2WZsLqc',
  aws_user_pools_web_client_id: '4van38hstleovrjfjarouh8mf1',
  oauth: {
    domain: 'dev-ridecare.auth.eu-central-1.amazoncognito.com',
    scope: ['phone', 'email', 'profile', 'openid', 'aws.cognito.signin.user.admin'],
//    redirectSignIn: 'https://tube.dev.bosch-ridecare.com/recording-overview',
//    redirectSignOut: 'https://tube.dev.bosch-ridecare.com/login',
    redirectSignIn: 'http://localhost:4200/recording-overview',
    redirectSignOut: 'http://localhost:4200/login',
    responseType: 'code', // or 'token', note that REFRESH token will only be generated when the responseType is code
  },
};

export default awsmobile;
