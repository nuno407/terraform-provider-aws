"""Helper module to create IMU data"""
import random
from datetime import datetime, timedelta
from typing import Optional, Tuple

from pytz import UTC

from base.model.artifacts.api_messages import (IMUProcessedData, IMUSample,
                                               IMUSource)

MIN_VALUE = -10.9
MAX_VALUE = 10.5
DEFAULT_FPS = 100.0
DEFAULT_SECONDS = 1.0


class ImuTestDataBuilder:
    """ Helper class to build IMU test data. """
    _fps: Optional[float] = None
    _length_in_seconds: Optional[float] = None
    _num_frames: Optional[int] = None
    _tenant_id: str = "tenant"
    _device_id: str = "device"
    _start_timestamp: datetime = datetime.now(tz=UTC) - timedelta(days=1)

    def with_frames(self, num_frames: int) -> "ImuTestDataBuilder":
        """ Sets the number of frames to generate. """
        self._num_frames = num_frames
        return self

    def with_fps(self, fps: float) -> "ImuTestDataBuilder":
        """ Sets the fps for the generated frames' timestamps. """
        self._fps = fps
        return self

    def with_seconds(self, seconds: float) -> "ImuTestDataBuilder":
        """ Sets the length of the generated data in seconds. """
        self._length_in_seconds = seconds
        return self

    def with_tenant_id(self, tenant: str) -> "ImuTestDataBuilder":
        """ Sets the tenant id for the generated data. """
        self._tenant_id = tenant
        return self

    def with_device_id(self, device: str) -> "ImuTestDataBuilder":
        """ Sets the device id for the generated data. """
        self._device_id = device
        return self

    def with_start_time(self, time: datetime) -> "ImuTestDataBuilder":
        """ Sets the start time for the generated data. """
        self._start_timestamp = time
        return self

    def __get_settings(self) -> Tuple[float, float, int]:
        if self._num_frames is None:
            fps = self._fps or DEFAULT_FPS
            secs = self._length_in_seconds or DEFAULT_SECONDS
            frames = round(secs * fps)
        else:
            frames = self._num_frames
            if self._fps is None and self._length_in_seconds is None:
                fps = DEFAULT_FPS
                secs = DEFAULT_SECONDS
            elif self._fps is None and self._length_in_seconds is not None:
                fps = self._num_frames / self._length_in_seconds
                secs = self._length_in_seconds
            else:
                secs = self._num_frames / (self._fps or 0)
                fps = self._fps or 0
        return fps, secs, frames

    def build(self) -> IMUProcessedData:
        """ Builds the IMU test data. """
        fps, _, frames = self.__get_settings()

        source = IMUSource(tenant=self._tenant_id, device_id=self._device_id)
        timestep = timedelta(seconds=1) / fps
        imu_data: list[IMUSample] = []
        for num_frame in range(0, frames):
            frame_ts = self._start_timestamp + num_frame * timestep
            imu_values: dict[str, float] = {}
            for attrib in ["gyr_y_mean",
                           "gyr_x_var",
                           "gyr_z_max",
                           "acc_z_var",
                           "acc_z_max",
                           "gyr_y_var",
                           "acc_y_min",
                           "gyr_z_var",
                           "acc_z_mean",
                           "acc_x_max",
                           "acc_y_mean",
                           "gyr_x_mean",
                           "acc_y_max",
                           "gyr_y_min",
                           "acc_y_var",
                           "acc_z_min",
                           "acc_x_mean",
                           "gyr_x_max",
                           "acc_x_min",
                           "gyr_z_mean",
                           "gyr_x_min",
                           "gyr_y_max",
                           "acc_x_var",
                           "gyr_z_min"]:
                imu_values[attrib] = random.uniform(MIN_VALUE, MAX_VALUE)
            imu_data.append(IMUSample(source=source, timestamp=int(frame_ts.timestamp() * 1000), **imu_values))
        return IMUProcessedData(root=imu_data)
