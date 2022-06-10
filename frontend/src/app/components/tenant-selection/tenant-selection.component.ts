import { Component, Input, OnInit } from '@angular/core';
import { Auth } from 'aws-amplify';
import { User } from 'src/app/components/login/login.interfaces';
import { TenantService } from 'src/app/core/services/tenant.service';
import { Router } from '@angular/router';
import { MatDialogRef } from '@angular/material/dialog';

@Component({
  selector: 'app-tenant-selection',
  templateUrl: './tenant-selection.component.html',
  styleUrls: ['./tenant-selection.component.scss'],
})
export class TenantSelectionComponent implements OnInit {
  @Input()
  tenants: string[] = [];

  @Input()
  selectedTenant: string;
  constructor(private _matDialogRef: MatDialogRef<TenantSelectionComponent>, public tenantService: TenantService, public router: Router) {}

  /**Aplication state */
  ngOnInit(): void {
    Auth.currentAuthenticatedUser().then((user: User) => {
      user?.signInUserSession?.accessToken?.payload['cognito:groups']?.forEach((group) => {
        if (!group.includes('eu-central')) {
          this.tenants.push(group);
        }
      });
      if (this.tenants.length === 1) {
        this.selectedTenant = this.tenants[0];
      }
    });
  }

  selectTenant() {
    this.tenantService.updateActiveTenant(this.selectedTenant);
    this._matDialogRef.close();
  }

  closeDialog() {
    this._matDialogRef.close();
  }
}
