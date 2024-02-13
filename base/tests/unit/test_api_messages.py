"Test metadata artifacts"
import pytest
from datetime import timedelta
from base.model.artifacts.api_messages import VideoSignalsData, IMUProcessedData, SignalsFrame, ConfiguredBaseModel
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
    @pytest.mark.parametrize("signals_json,signals_dict",
                             [(load_data_str("test_signals.json"),
                               {"correlation_id": "DATANAUTS_DEV_02_InteriorRecorder_89983c99-8ff5-4eb1-9140-ca019e70c1c0_1680540223210_1680540250651",
                                "artifact_name": "video_signals_data",
                                 "data": {timedelta(seconds=1): {"CameraViewShifted": False,
                                                                 "CameraVerticalShifted": 0.988,
                                                                 "CameraViewBlocked": 0.972},
                                          timedelta(seconds=2): {"CameraViewShifted": False,
                                                                 "CameraVerticalShifted": 0.953,
                                                                 "CameraViewBlocked": 0.97}},
                                 "aggregated_metadata": {"foo": 10},
                                 "tenant_id": "datanauts",
                                 "video_raw_s3_path": "s3://raw/foo/bar.something"})],
                             ids=["signals_1"])
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

        assert imu_parsed == imu_python

    @pytest.mark.unit
    @pytest.mark.parametrize("signals_json,signals_pydantic", [
        (
            {
                "CameraViewShifted":False,
                "CameraVerticalShifted":0.988,
                "DrivingStatus":0,
            },
            SignalsFrame(DrivingStatus=0,CameraVerticalShifted=0.988,CameraViewShifted=False),  # type: ignore
        )
    ], ids=["test_signals_frame"])
    def test_signals_frame(self, signals_json: str, signals_pydantic: SignalsFrame):
        """
        Test signals frame
        """
        model = SignalsFrame.model_validate(signals_json)
        model_dump = model.model_dump()

        assert model == signals_pydantic
        assert isinstance(model_dump["CameraViewShifted"],bool)
        assert isinstance(model_dump["DrivingStatus"],int)
        assert model_dump == signals_json
