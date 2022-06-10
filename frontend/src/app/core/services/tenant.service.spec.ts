import { TestBed } from '@angular/core/testing';
import { TenantService } from './tenant.service';

describe('TenantService', () => {
  let service: TenantService;

  beforeEach(() => {
    TestBed.configureTestingModule({ providers: [TenantService] });
    service = TestBed.inject(TenantService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('it should update the tenant', (done: DoneFn) => {
    spyOn(service, 'updateActiveTenant').and.callThrough();
    const tenant = 'TEST_TENANT';
    service.updateActiveTenant(tenant);
    service.getTenant().subscribe((value) => {
      expect(value).toBe(tenant);
      done();
    });
    expect(service.updateActiveTenant).toHaveBeenCalled();
  });
});
