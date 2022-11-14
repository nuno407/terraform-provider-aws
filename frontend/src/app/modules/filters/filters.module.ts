import { CUSTOM_ELEMENTS_SCHEMA, NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatMenuModule } from '@angular/material/menu';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatIconModule } from '@angular/material/icon';
import { MatRadioModule } from '@angular/material/radio';
import { BciComponentModule, BciCoreModule, BciLayoutModule, BciSharedModule } from '@bci-web-core/core';
import { environment } from '../../../environments/environment';
import { MatInputModule } from '@angular/material/input';
import { TranslateModule } from '@ngx-translate/core';
import { FilterItemDirective } from './directives/filter-item.directive';
import { FilterStringComponent } from './components/filter-string/filter-string.component';
import { FilterDatetimeComponent } from './components/filter-datetime/filter-datetime.component';
import { FilterMultiComponent } from './components/filter-multi/filter-multi.component';

@NgModule({
  declarations: [FilterStringComponent, FilterDatetimeComponent, FilterMultiComponent, FilterItemDirective],
  exports: [FilterStringComponent, FilterDatetimeComponent, FilterMultiComponent, FilterItemDirective],
  imports: [
    BciCoreModule.forRoot({
      prod_environment: environment.name == 'docker',
      core_config_url: '/assets/config/config.json',
    }),
    BciComponentModule,
    BciLayoutModule,
    BciSharedModule,
    CommonModule,
    MatMenuModule,
    MatCheckboxModule,
    MatIconModule,
    MatRadioModule,
    MatInputModule,
    TranslateModule,
  ],
  schemas: [CUSTOM_ELEMENTS_SCHEMA],
})
export class FiltersModule {}
