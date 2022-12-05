import { NoCommaPipe } from './no-comma.pipe';

it('should strip all commas', () => {
  const pipe = new NoCommaPipe();
  const noComma = pipe.transform('This, has, many, commas');

  expect(noComma).toEqual('This has many commas');
});

it('should return empty string on null', () => {
  const pipe = new NoCommaPipe();
  const noComma = pipe.transform(null);

  expect(noComma).toEqual('');
});
