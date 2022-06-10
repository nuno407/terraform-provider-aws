import { fakeAsync, inject, TestBed, tick } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController, TestRequest } from '@angular/common/http/testing';
import { HttpClient, HTTP_INTERCEPTORS } from '@angular/common/http';
import { JwtInterceptor } from './jwt.interceptor';
import Auth from '@aws-amplify/auth';

describe('JwtInterceptor', () => {
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        {
          provide: HTTP_INTERCEPTORS,
          useClass: JwtInterceptor,
          multi: true,
        },
      ],
    });

    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it('should add an Authorization header', inject(
    [HttpClient],
    fakeAsync((http: HttpClient) => {
      Auth.currentSession = jasmine.createSpy().and.returnValue(
        Promise.resolve({
          accessToken: { jwtToken: '123' },
        })
      );

      http.get('/dummy').subscribe();
      tick();
      const httpRequest: TestRequest = httpMock.expectOne('/dummy');
      expect(httpRequest.request.headers.has('Authorization')).toBeTruthy();
    })
  ));
});
