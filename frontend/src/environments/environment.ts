// This file can be replaced during build by using the `fileReplacements` array.
// `ng build ---prod` replaces `environment.ts` with `environment.prod.ts`.
// The list of file replacements can be found in `angular.json`.
import { Configuration } from '@azure/msal-browser';

export interface AppConfig {
  name: string;
  api: string;
  protectedRoutes: string[];
  msalConfig: Configuration;
}

export const environment: AppConfig = {
  name: window['env']['environmentName'],
  api: window['env']['api'],
  protectedRoutes: window['env']['protectedRoutes'],
  msalConfig: window['env']['msalConfig'],
};
