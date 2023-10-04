from dataclasses import dataclass


@dataclass
class APIEndpoints:
    RC_SIGNALS_VIDEO="/ridecare/signals/video"
    RC_SIGNALS_SNAPSHOT="/ridecare/signals/snapshot"
    RC_VIDEO="/ridecare/video"
    RC_SNAPSHOT="/ridecare/snapshots"
    RC_IMU_VIDEO="/ridecare/imu/video"
    RC_PIPELINE_ANON_VIDEO="/ridecare/pipeline/anonymize/video"
    RC_PIPELINE_ANON_SNAPSHOT="/ridecare/pipeline/anonymize/snapshot"
    RC_PIPELINE_CHC_VIDEO="/ridecare/pipeline/chc/video"
    RC_PIPELINE_CHC_SNAPSHOT="/ridecare/pipeline/chc/snapshot"
    RC_PIPELINE_CHC_STATUS="/ridecare/pipeline/chc/status"
    RC_PIPELINE_CHC_OPERATOR="/ridecare/operator"
    RC_PIPELINE_CHC_EVENT="/ridecare/event"
