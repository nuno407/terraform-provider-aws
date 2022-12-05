import { CHART_DATE_START_BASELINE as CHART_DATE_BASELINE, FPS } from '../constants';
import { Snapshot } from './snapshots';

describe('Snapshots', () => {
  it('should create snapshot', () => {
    let testSnapshotEpoch = Date.now();
    let testVideoStart = new Date(testSnapshotEpoch - 1000 * 60); // one minute before
    let snapshot = new Snapshot('snapshot1', testSnapshotEpoch, testVideoStart);
    let expectedRecordTime = new Date(testSnapshotEpoch);
    let expectedVideoTime = new Date(CHART_DATE_BASELINE.getTime() + (snapshot.recordTime.getTime() - testVideoStart.getTime()));
    let expectedFrame = Math.round((Math.abs(expectedVideoTime.getTime() - CHART_DATE_BASELINE.getTime()) / 1000) * FPS);

    expect(snapshot.name).toEqual('snapshot1');
    expect(snapshot.enabled).toBeTrue();
    expect(snapshot.recordTime).toEqual(expectedRecordTime);
    expect(snapshot.videoTime).toEqual(expectedVideoTime);
    expect(snapshot.frame).toBe(expectedFrame);
  });
});
