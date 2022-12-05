import { TestBed } from '@angular/core/testing';

import { ApiVideoCallService } from './api-video-call.service';
import { HttpClientModule } from '@angular/common/http';
import { RouterTestingModule } from '@angular/router/testing';
import { routes } from '../../app-routing.module';

describe('ApiVideoCallService', () => {
  let service: ApiVideoCallService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [RouterTestingModule.withRoutes(routes), HttpClientModule],
    }).compileComponents();
    service = TestBed.inject(ApiVideoCallService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
