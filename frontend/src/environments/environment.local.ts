// This file can be replaced during build by using the `fileReplacements` array.
// `ng build ---prod` replaces `environment.ts` with `environment.prod.ts`.
// The list of file replacements can be found in `angular.json`.
import { msalConfig } from '../assets/azure/oauth-config-dev';

export const environment = {
  name: 'dev',
  api: 'http://localhost:5000/',
  protectedRoutes: ['/*'],
  msalConfig: msalConfig,
};
