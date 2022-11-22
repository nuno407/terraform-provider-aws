import { DecimalPipe } from '@angular/common';
import { HttpBackend, HttpClient, HTTP_INTERCEPTORS } from '@angular/common/http';
import { CUSTOM_ELEMENTS_SCHEMA, NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatDividerModule } from '@angular/material/divider';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatIconModule } from '@angular/material/icon';
import { MatInputModule } from '@angular/material/input';
import { MatListModule } from '@angular/material/list';
import { MatMenuModule } from '@angular/material/menu';
import { MatPaginatorModule } from '@angular/material/paginator';
import { MatRadioModule } from '@angular/material/radio';
import { MatSelectModule } from '@angular/material/select';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { MatSliderModule } from '@angular/material/slider';
import { MatTableModule } from '@angular/material/table';
import { MatTabsModule } from '@angular/material/tabs';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import {
  MsalBroadcastService,
  MsalGuard,
  MsalInterceptor,
  MsalModule,
  MsalRedirectComponent,
  MsalService,
  MSAL_GUARD_CONFIG,
  MSAL_INSTANCE,
  MSAL_INTERCEPTOR_CONFIG,
} from '@azure/msal-angular';
import { BciComponentModule, BciCoreModule, BciLayoutModule, BciSharedModule } from '@bci-web-core/core';
import { TranslateLoader, TranslateModule } from '@ngx-translate/core';
import { TranslateHttpLoader } from '@ngx-translate/http-loader';
import 'chartjs-plugin-zoom';
import 'hammerjs';
import { environment } from '../environments/environment';
import { AdminComponent } from './admin/admin.component';
import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { AuthService } from './auth/auth.service';
import { AuthGuardService } from './auth/authguard.service';
import { MSALGuardConfigFactory, MSALInstanceFactory, MSALInterceptorConfigFactory } from './auth/msalconfig';
import { LanguageSelectorComponent } from './components/language-selector/language-selector.component';
import { LineChartComponent } from './components/line-chart/line-chart.component';
import { RecordingDetailComponent } from './components/recording-detail/recording-detail.component';
import { RecordingOverviewComponent } from './components/recording-overview/recording-overview.component';
import { ActivitiesComponent } from './components/sidebar/activities/activities.component';
import { LabelEntryComponent } from './components/sidebar/label-entry/label-entry.component';
import { SidebarComponent } from './components/sidebar/sidebar.component';
import { SignalSelectionComponent } from './components/signal-selection/signal-selection.component';
import { TenantSelectionComponent } from './components/tenant-selection/tenant-selection.component';
import { VideoPlayerComponent } from './components/video-player/video-player.component';
import { TenantService } from './core/services/tenant.service';
import { FiltersModule } from './modules/filters/filters.module';
import { NoCommaPipe } from './pipes/no-comma.pipe';

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
    MsalModule,
  ],
  schemas: [CUSTOM_ELEMENTS_SCHEMA],
  providers: [
    {
      provide: HTTP_INTERCEPTORS,
      useClass: MsalInterceptor,
      multi: true,
    },
    {
      provide: MSAL_INSTANCE,
      useFactory: MSALInstanceFactory,
    },
    {
      provide: MSAL_GUARD_CONFIG,
      useFactory: MSALGuardConfigFactory,
    },
    {
      provide: MSAL_INTERCEPTOR_CONFIG,
      useFactory: MSALInterceptorConfigFactory,
    },
    AuthGuardService,
    TenantService,
    AuthService,
    MsalService,
    MsalGuard,
    MsalBroadcastService,
    {
      provide: MatDialogRef,
      useValue: {},
    },
    DecimalPipe,
  ],
  bootstrap: [AppComponent, MsalRedirectComponent],
})
export class AppModule {}
