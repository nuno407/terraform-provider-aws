"Test metadata artifacts"
import pytest
import pandas as pd
from datetime import timedelta
from base.model.artifacts.api_messages import VideoSignalsData, IMUProcessedData, SignalsFrame, ConfiguredBaseModel
from base.testing.utils import load_relative_str_file, load_relative_json_file, get_abs_path


def load_data_str(file_name: str) -> str:
    """Load video signals"""
    path = f"artifacts/{file_name}"
    return load_relative_str_file(__file__, path)


def get_data_path(file_name: str) -> str:
    """Get data path"""
    path = f"artifacts/{file_name}"
    return get_abs_path(__file__, path)


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
    def test_imu(self):
        """
        Test IMU
        """
        abs_path = get_data_path("test_imu.json")
        df = pd.read_json(abs_path, orient="records")
        model = IMUProcessedData.validate(df)

        dtypes = model.dtypes
        dtypes.pop("source")
        dtypes.pop("timestamp")

        assert model.timestamp.dtype == "datetime64[ns, UTC]"
        assert model.source.dtype == "object"
        assert dtypes.values.all() == "float64"

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
