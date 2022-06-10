import { Activity } from './activity';

export class Label {
  start: {
    frame: number;
    seconds: number;
  };
  end: {
    frame: number;
    seconds: number;
  };
  activities: Activity;
  visibility: boolean;

  constructor(start?, end?, activities?) {
    this.start = {
      frame: (start && start.frame) || 0,
      seconds: (start && start.seconds) || 0,
    };
    this.end = {
      frame: (end && end.frame) || 0,
      seconds: (end && end.seconds) || 0,
    };
    this.activities = activities || new Activity();
    this.visibility = true;
  }
}
