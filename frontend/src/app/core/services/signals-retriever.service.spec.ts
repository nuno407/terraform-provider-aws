import { TestBed } from '@angular/core/testing';

import { SignalsRetrieverService } from './signals-retriever.service';
import { HttpClientModule } from '@angular/common/http';
import { RouterTestingModule } from '@angular/router/testing';
import { routes } from '../../app-routing.module';

describe('SignalsRetrieverService', () => {
  let service: SignalsRetrieverService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [RouterTestingModule.withRoutes(routes), HttpClientModule],
    });
    service = TestBed.inject(SignalsRetrieverService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
