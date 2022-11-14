import { FPS, CHART_DATE_START_BASELINE as CHART_DATE_BASELINE } from '../constants';
export class SnapshotParser {
  public static PATH_DELIMITER = '_';

  static parse(snapshotPaths: string[], videoStartTimestamp: string) {
    let videoStartTime = new Date(videoStartTimestamp + '+0000');
    let snapshots = new Array<Snapshot>();

    for (var path of snapshotPaths) {
      let path_parts = path.split(this.PATH_DELIMITER);
      let epoch = path_parts.at(-1);
      let snapshot = new Snapshot(`${path}.jpeg`, +epoch, videoStartTime);
      snapshots.push(snapshot);
    }
    return snapshots;
  }
}

export class Snapshot {
  name: string;
  recordTime: Date;
  videoTime: Date;
  frame: number;
  enabled: boolean;

  constructor(name: string, snapshotEpoch: number, videoStartTime: Date) {
    this.name = name;
    this.enabled = true;
    this.recordTime = new Date(snapshotEpoch);
    this.videoTime = new Date(CHART_DATE_BASELINE.getTime() + (this.recordTime.getTime() - videoStartTime.getTime()));
    this.frame = Math.round((Math.abs(this.videoTime.getTime() - CHART_DATE_BASELINE.getTime()) / 1000) * FPS);
  }
}
