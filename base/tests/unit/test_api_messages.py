"Test metadata artifacts"
import pytest
from datetime import timedelta
from base.model.metadata.api_messages import VideoSignalsData, IMUProcessedData
from base.testing.utils import load_relative_str_file, load_relative_json_file


def load_data_str(file_name: str) -> str:
    """Load video signals"""
    path = f"artifacts/{file_name}"
    return load_relative_str_file(__file__, path)


def load_data_dict(file_name: str) -> dict:
    """Load video signals"""
    path = f"artifacts/{file_name}"
    return load_relative_json_file(__file__, path)


class TestMetadataArtifacts:

    @pytest.mark.unit
    @pytest.mark.parametrize("signals_json,signals_dict", [
        (
            load_data_str("test_signals.json"),
            {
                timedelta(seconds=1, microseconds=6000): {
                    "CameraViewShifted": False,
                    "Gnss_has_fix": True,
                    "mock_int_value": 1,
                    "mock_int_float": 1.0
                },
                timedelta(seconds=2, microseconds=24000): {
                    "CameraViewShifted": False
                }
            }
        )
    ], ids=["signals_1"])
    def test_video_signals(self, signals_json: str, signals_dict: dict):
        """
        Test video signals
        """
        model = VideoSignalsData.model_validate_json(signals_json)

        assert model.model_dump() == signals_dict

    @pytest.mark.unit
    @pytest.mark.parametrize("imu_json,imu_python", [
        (
            load_data_str("test_imu.json"),
            load_data_dict("test_imu.json"),
        )
    ], ids=["imu_1"])
    def test_imu(self, imu_json: str, imu_python: dict):
        """
        Test IMU
        """
        model = IMUProcessedData.model_validate_json(imu_json)
        imu_parsed = model.model_dump()

        for sample in imu_parsed:
            sample["timestamp"] = sample["timestamp"].timestamp() * 1000

        assert imu_parsed == imu_python
