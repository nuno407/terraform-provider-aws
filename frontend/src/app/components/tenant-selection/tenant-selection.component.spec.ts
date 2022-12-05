import { ComponentFixture, TestBed } from '@angular/core/testing';
import { TenantSelectionComponent } from './tenant-selection.component';
import { TranslateModule } from '@ngx-translate/core';
import { RouterModule } from '@angular/router';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { MatDialogRef } from '@angular/material/dialog';
import { TenantService } from 'src/app/core/services/tenant.service';

class TenantServiceStub {
  updateActiveTenant(tenant: string) {}
}

describe('TenantSelectionComponent', () => {
  let component: TenantSelectionComponent;
  let fixture: ComponentFixture<TenantSelectionComponent>;

  const dialogMock = {
    close: () => {},
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [TenantSelectionComponent],
      imports: [TranslateModule.forRoot(), RouterModule.forRoot([])],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
      providers: [
        { provide: MatDialogRef, useValue: dialogMock },
        { provide: TenantService, useClass: TenantServiceStub },
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TenantSelectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize the tenants with undefined content', async () => {
    await fixture.whenStable();
    expect(component.tenants).toEqual([]);
    expect(component.selectedTenant).toBeUndefined();
  });

  it('should select a tenant', () => {
    spyOn(component.tenantService, 'updateActiveTenant').and.callThrough();
    component.selectedTenant = 'TEST_TENANT';
    const dialog = fixture.debugElement.injector.get(MatDialogRef);
    const dialogSpy = spyOn(dialog, 'close').and.callThrough();
    component.selectTenant();
    expect(component.tenantService.updateActiveTenant).toHaveBeenCalledWith('TEST_TENANT');
    expect(dialogSpy).toHaveBeenCalled();
  });

  it('should close the dialog', () => {
    const dialog = fixture.debugElement.injector.get(MatDialogRef);
    const dialogSpy = spyOn(dialog, 'close').and.callThrough();
    component.closeDialog();
    expect(dialogSpy).toHaveBeenCalled();
  });
});
