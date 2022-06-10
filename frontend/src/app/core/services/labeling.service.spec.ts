import { TestBed } from '@angular/core/testing';

import { LabelingService } from './labeling.service';

describe('LabelingService', () => {
  let service: LabelingService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(LabelingService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
