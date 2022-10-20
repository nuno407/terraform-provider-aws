// This file can be replaced during build by using the `fileReplacements` array.
// `ng build ---prod` replaces `environment.ts` with `environment.prod.ts`.
// The list of file replacements can be found in `angular.json`.
import awsconfigdev from '../assets/aws/aws-exports-dev.js';

export const environment = {
  name: 'local',
  api: 'http://localhost:7777/',
  identityProvider: 'Azure-RideCareStage',
  amplifyConfig: awsconfigdev,
};
