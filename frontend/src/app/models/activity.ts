export class Activity {
  nonPhysicalAggression = {
    values: [
      {
        value: 'None',
      },
      {
        identifier: 1,
        value: 'Minor',
      },
      {
        identifier: 2,
        value: 'Moderate',
      },
      {
        identifier: 3,
        value: 'Severe',
      },
    ],
    selected: 0,
  };
  physicalAggression = {
    values: [
      {
        value: 'None',
      },
      {
        identifier: 4,
        value: 'Minor',
      },
      {
        identifier: 5,
        value: 'Moderate',
      },
      {
        identifier: 6,
        value: 'Severe',
      },
    ],
    selected: 0,
    weapon: {
      identifier: 7,
      value: false,
    },
  };
  occupants = 0;
  night = false;
  driving = true;
}
