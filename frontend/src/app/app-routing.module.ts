import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { AuthGuardService } from './auth/authguard.service';
import { TenantSelectionComponent } from './components/tenant-selection/tenant-selection.component';
import { AdminComponent } from './admin/admin.component';
import { RecordingOverviewComponent } from './components/recording-overview/recording-overview.component';

export const routes: Routes = [
  {
    path: '',
    component: AdminComponent,
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
    ],
  },
  { path: '**', redirectTo: '/recording-overview' },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule],
})
export class AppRoutingModule {}
