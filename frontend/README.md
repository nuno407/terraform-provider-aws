# Ridecare Dev Cloud Frontend

# Application development

## Requirements
Currently we are using for this project an internal npm registry. This registry is contains some npm packages internally developed by Bosch as well as an "mirror" of npmjs registry (configured as upstream). To access and use this registry you should be registered as "Social Coding platform" user (with "Social Coding Access" and "Social Coding BIOS Access"). Go [here](http://rb-cae.de.bosch.com/ServiceRegistration/?SocialCoding) and request the "Social Coding Access" and "Social Coding BIOS Access".

Registry: https://artifactory.boschdevcloud.com/artifactory/api/npm/lab000003-bci-npm-virtual/

To setup this registry in you local machine please do:
```bash
npm config set registry https://artifactory.boschdevcloud.com/artifactory/api/npm/lab000003-bci-npm-virtual/
npm login # use your nt-user as username and the Identity token as the password. To generate an Identity Token please go https://artifactory. boschdevcloud.com, login with SAML, click on your name (top right corner) and click on "Edit Profile". Then click on "Generate an Identity Token" and follow the steps to generate te key. Then use it as the password on the prompt.
```

## Development Server

You can run a development server which is connecting to one of several available backends by using these commands:

| Command             | Description                                                                              |
|:--------------------|:-----------------------------------------------------------------------------------------|
| npm run start:dev   | Build local environment for testing against DEV backend                                  |
| npm run start:qa    | Build local environment for testing against QA backend                                   |
| npm run start:local | Build local environment for testing against local backend running on localhost port 7777 |

To start a dev server run `npm run start`. It will automatically detect changes to the source files as soon as they are saved and rebuild on demand.

Those script / command definitions can be found in the `package.json`.

## How to run the tests locally

To run the tests please do:

`npm run test-headless`

Please note that if you are under `WSL` you should have chrome (installation steps [here](https://scottspence.com/posts/use-chrome-in-ubuntu-wsl)) installed. and then set the `CHROME_BIN` variable.

`CHROME_BIN=/usr/bin/google-chrome npm run test-headless`

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
