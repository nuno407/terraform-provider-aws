import { enableProdMode } from '@angular/core';
import { platformBrowserDynamic } from '@angular/platform-browser-dynamic';
import { applyPolyfills, defineCustomElements } from '@bci-web-core/web-components/loader';

import { AppModule } from './app/app.module';
import { environment } from './environments/environment';
import { Amplify } from 'aws-amplify';

if (environment.name == 'docker') {
  enableProdMode();
}

Amplify.configure({
  ...environment.amplifyConfig,
});

platformBrowserDynamic()
  .bootstrapModule(AppModule)
  .catch((err) => console.error(err));

applyPolyfills().then(() => {
  defineCustomElements(window);
});
