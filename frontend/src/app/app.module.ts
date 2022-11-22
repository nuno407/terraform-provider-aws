import { BrowserModule } from '@angular/platform-browser';
import { CUSTOM_ELEMENTS_SCHEMA, NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { BciComponentModule, BciCoreModule, BciLayoutModule, BciSharedModule } from '@bci-web-core/core';
import { environment } from '../environments/environment';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { MatTableModule } from '@angular/material/table';
import { MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatSelectModule } from '@angular/material/select';
import { MatInputModule } from '@angular/material/input';
import { MatMenuModule } from '@angular/material/menu';
import { MatIconModule } from '@angular/material/icon';
import { MatRadioModule } from '@angular/material/radio';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatPaginatorModule } from '@angular/material/paginator';
import { HTTP_INTERCEPTORS, HttpBackend, HttpClient } from '@angular/common/http';
import { FiltersModule } from './modules/filters/filters.module';
import { TranslateLoader, TranslateModule } from '@ngx-translate/core';
import { TranslateHttpLoader } from '@ngx-translate/http-loader';
import { LanguageSelectorComponent } from './components/language-selector/language-selector.component';
import { AuthGuardService } from './auth/authguard.service';
import { TenantService } from './core/services/tenant.service';
import { AuthService } from './auth/auth.service';
import { TenantSelectionComponent } from './components/tenant-selection/tenant-selection.component';
import { MatListModule } from '@angular/material/list';
import { MatDividerModule } from '@angular/material/divider';
import { AdminComponent } from './admin/admin.component';
import { MatTabsModule } from '@angular/material/tabs';
import { RecordingOverviewComponent } from './components/recording-overview/recording-overview.component';
import { RecordingDetailComponent } from './components/recording-detail/recording-detail.component';
import { VideoPlayerComponent } from './components/video-player/video-player.component';
import { NoCommaPipe } from './pipes/no-comma.pipe';
import { SidebarComponent } from './components/sidebar/sidebar.component';
import { LabelEntryComponent } from './components/sidebar/label-entry/label-entry.component';
import { DecimalPipe } from '@angular/common';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { ActivitiesComponent } from './components/sidebar/activities/activities.component';
import { MatSliderModule } from '@angular/material/slider';
import { LineChartComponent } from './components/line-chart/line-chart.component';
import 'hammerjs';
import 'chartjs-plugin-zoom';
import { SignalSelectionComponent } from './components/signal-selection/signal-selection.component';

export function HttpLoaderFactory(handler: HttpBackend) {
  const http = new HttpClient(handler);
  return new TranslateHttpLoader(http, './assets/i18n/', '.json');
}
@NgModule({
  declarations: [
    AppComponent,
    LanguageSelectorComponent,
    TenantSelectionComponent,
    AdminComponent,
    RecordingOverviewComponent,
    RecordingDetailComponent,
    VideoPlayerComponent,
    NoCommaPipe,
    SidebarComponent,
    LabelEntryComponent,
    ActivitiesComponent,
    LineChartComponent,
    SignalSelectionComponent,
  ],
  imports: [
    BrowserModule,
    ReactiveFormsModule,
    AppRoutingModule,
    BciCoreModule.forRoot({
      prod_environment: environment.name == 'docker',
    }),
    TranslateModule.forRoot({
      loader: {
        provide: TranslateLoader,
        useFactory: HttpLoaderFactory,
        deps: [HttpBackend],
      },
    }),
    BciLayoutModule,
    BciSharedModule,
    BrowserAnimationsModule,
    MatTableModule,
    MatMenuModule,
    MatIconModule,
    MatRadioModule,
    MatCheckboxModule,
    MatDialogModule,
    MatDividerModule,
    MatSelectModule,
    MatSliderModule,
    MatPaginatorModule,
    BciComponentModule,
    MatFormFieldModule,
    MatInputModule,
    FiltersModule,
    MatListModule,
    MatTabsModule,
    MatSlideToggleModule,
    MatExpansionModule,
  ],
  schemas: [CUSTOM_ELEMENTS_SCHEMA],
  providers: [
    AuthGuardService,
    TenantService,
    AuthService,
    {
      provide: MatDialogRef,
      useValue: {},
    },
    DecimalPipe,
  ],
  bootstrap: [AppComponent],
})
export class AppModule {}
