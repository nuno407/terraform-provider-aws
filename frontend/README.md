# Ridecare Operator Webportal

# Application development

You can run a development server which is connecting to one of several available backends by using these commands:

| Command             | Description                                                                              |
|:--------------------|:-----------------------------------------------------------------------------------------|
| npm run start:dev   | Build local environment for testing against DEV backend                                  |
| npm run start:qa    | Build local environment for testing against QA backend                                   |
| npm run start:local | Build local environment for testing against local backend running on localhost port 7777 |

To start a dev server run `npm run start`. It will automatically detect changes to the source files as soon as they are saved and rebuild on demand.

Those script / command definitions can be found in the `package.json`.

# Deployment

For a deployment in a docker environment, e.g. Kubernetes or docker compose, the Dockerfile build needs to be executed first.
This will build the default configuration of Angular, which will read its configuration from environment variables at the start of the container.
The following environment variables need to be filled:

| Variable          | Description                                                                                           | Example value (dev environment)                                          |
|:------------------|:------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------|
| `API_URL`         | URL of the backend to use                                                                             | `https://ai-dev.bosch-ridecare.com/api/`                                 |
| `AZURE_CLIENT_ID` | Azure Application (Client) ID                                                                         | `b74f0fea-4e4a-4785-886a-96c1922dfb7b`                                   |
| `AZURE_AUTHORITY` | Azure Authority (Issuer + Tenant ID)                                                                  | `https://login.microsoftonline.com/0ae51e19-07c8-4e4b-bb6d-648ee58410f4` |
| `OWN_URL`         | URL under which the frontend is running, used to set the redirections before and after authentication | `https://ai-dev.bosch-ridecare.com`                                      |

# Framework information

## Web Core Documentation

<http://atmo1opcondemo.de.bosch.com/WebCore/>

## Further information

<https://inside-docupedia.bosch.com/confluence/display/WC>
