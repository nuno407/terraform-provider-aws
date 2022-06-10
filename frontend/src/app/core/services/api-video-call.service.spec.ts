import { TestBed } from '@angular/core/testing';

import { ApiVideoCallService } from './api-video-call.service';

describe('ApiVideoCallService', () => {
  let service: ApiVideoCallService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(ApiVideoCallService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
