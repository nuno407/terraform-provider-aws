import { TestBed } from '@angular/core/testing';

import { SignalsRetrieverService } from './signals-retriever.service';

describe('SignalsRetrieverService', () => {
  let service: SignalsRetrieverService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(SignalsRetrieverService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
