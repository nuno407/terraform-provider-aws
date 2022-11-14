(function(window) {
    window["env"] = window["env"] || {};

    // Environment variables
    window["env"]["environmentName"] = "docker";
    window["env"]["api"] = "${API_URL}";
    window["env"]["identityProvider"] = "${IDENTITY_PROVIDER}";

    // AWS Amplify configuration
    let amplifyConfig = {
      aws_project_region: "${AWS_COGNITO_REGION}",
      aws_cognito_identity_pool_id: "${AWS_COGNITO_IDENTITY_POOL_ID}",
      aws_cognito_region: "${AWS_COGNITO_REGION}",
      aws_user_pools_id: "${AWS_COGNITO_USER_POOLS_ID}",
      aws_user_pools_web_client_id: "${AWS_COGNITO_USER_POOLS_WEB_CLIENT_ID}",
      oauth: {
        domain: "${AWS_COGNITO_OAUTH_DOMAIN}",
        scope: ['phone', 'email', 'profile', 'openid', 'aws.cognito.signin.user.admin'],
        redirectSignIn: "${OWN_URL}/recording-overview",
        redirectSignOut: '${OWN_URL}/login',
        responseType: 'code'
      },
    };

    window["env"]["amplifyConfig"] = amplifyConfig;

  })(this);
