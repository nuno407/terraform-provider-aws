// This file can be replaced during build by using the `fileReplacements` array.
// `ng build ---prod` replaces `environment.ts` with `environment.prod.ts`.
// The list of file replacements can be found in `angular.json`.

export const environment = {
  name: window['env']['environmentName'],
  api: window['env']['api'],
  identityProvider: window['env']['identityProvider'],
  amplifyConfig: window['env']['amplifyConfig'],
};
