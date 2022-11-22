import { enableProdMode } from '@angular/core';
import { platformBrowserDynamic } from '@angular/platform-browser-dynamic';
import { applyPolyfills, defineCustomElements } from '@bci-web-core/web-components/loader';

import { AppModule } from './app/app.module';
import { environment } from './environments/environment';

if (environment.name == 'docker') {
  enableProdMode();
}

platformBrowserDynamic()
  .bootstrapModule(AppModule)
  .catch((err) => console.error(err));

applyPolyfills().then(() => {
  defineCustomElements(window);
});
