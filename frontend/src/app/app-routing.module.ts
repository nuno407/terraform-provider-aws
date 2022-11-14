import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { LoginComponent } from './components/login/login.component';
import { AuthGuardService } from './auth/authguard.service';
import { TenantSelectionComponent } from './components/tenant-selection/tenant-selection.component';
import { AdminComponent } from './admin/admin.component';
import { RecordingOverviewComponent } from './components/recording-overview/recording-overview.component';
import { MfaComponent } from './components/mfa/mfa.component';

export const routes: Routes = [
  {
    path: 'login',
    component: LoginComponent,
  },
  {
    path: '',
    component: AdminComponent,
    canActivate: [AuthGuardService],
    children: [
      {
        path: 'recording-overview',
        component: RecordingOverviewComponent,
        canActivate: [AuthGuardService],
      },
      {
        path: 'tenant-selection',
        component: TenantSelectionComponent,
        canActivate: [AuthGuardService],
      },
      {
        path: 'mfa-settings',
        component: MfaComponent,
        canActivate: [AuthGuardService],
      },
    ],
  },
  { path: '**', redirectTo: '/login' },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule],
})
export class AppRoutingModule {}
