# Ridecare Operator Webportal

# Application development

You can run a development server which is connecting to one of several available backends by using these commands:

| Command             | Description                                                                              |
| :------------------ | :--------------------------------------------------------------------------------------- |
| npm run start:dev   | Build local environment for testing against DEV backend                                  |
| npm run start:qa    | Build local environment for testing against QA backend                                   |
| npm run start:local | Build local environment for testing against local backend running on localhost port 7777 |

To start a dev server run `npm run start`. It will automatically detect changes to the source files as soon as they are saved and rebuild on demand.

Those script / command definitions can be found in the `package.json`.

# Deployment

For a deployment in a docker environment, e.g. Kubernetes or docker compose, the Dockerfile build needs to be executed first.
This will build the default configuration of Angular, which will read its configuration from environment variables at the start of the container.
The following environment variables need to be filled:
| Variable                               | Description                                                                                           | Example value (dev environment)                     |
| :------------------------------------- | :---------------------------------------------------------------------------------------------------- | :-------------------------------------------------- |
| `API_URL`                              | URL of the backend to use                                                                             | `https://ai-dev.bosch-ridecare.com/api/`            |
| `IDENTITY_PROVIDER`                    | Name of the identity provider                                                                         | `Azure-RideCareStage`                               |
| `AWS_COGNITO_REGION`                   | Region for AWS Cognito                                                                                | `eu-central-1`                                      |
| `AWS_COGNITO_IDENTITY_POOL_ID`         | ID of the AWS Cognito Identity Pool to use for authentication                                         | `eu-central-1:21068fdd-2d6f-4b30-809b-91f0cae03503` |
| `AWS_COGNITO_USER_POOLS_ID`            | ID of the AWS Cognito User Pools to use for authentication                                            | `eu-central-1_Oc2WZsLqc`                            |
| `AWS_COGNITO_USER_POOLS_WEB_CLIENT_ID` | ID of the AWS Cognito User Pools web client to use for authentication                                 | `jk163jb8n5dgr1esr67rnkdmr`                         |
| `AWS_COGNITO_OAUTH_DOMAIN`             | OAuth domain under which AWS Cognito can be reached                                                   | `dev-ridecare.auth.eu-central-1.amazoncognito.com`  |
| `OWN_URL`                              | URL under which the frontend is running, used to set the redirections before and after authentication | `https://ai-dev.bosch-ridecare.com`                 |

# Framework information

## Web Core Documentation
<http://atmo1opcondemo.de.bosch.com/WebCore/>

## Further information
<https://inside-docupedia.bosch.com/confluence/display/WC>
