from pytest import fixture, mark
from selector.model import PreviewMetadataV063, FrameSignal
from datetime import datetime
import pytz


@mark.unit
class TestPreviewMetadata:

    @fixture(scope="session")
    def mminimal_preview_frame_timestamps(self) -> list[datetime]:
        return [
            datetime(2023, 5, 31, 14, 48, 24, 63000, tzinfo=pytz.UTC),
            datetime(2023, 5, 31, 14, 48, 24, 954000, tzinfo=pytz.UTC),
            datetime(2023, 5, 31, 14, 48, 25, 972000, tzinfo=pytz.UTC),
            datetime(2023, 5, 31, 14, 48, 26, 989000, tzinfo=pytz.UTC),
            datetime(2023, 5, 31, 14, 48, 27, 943000, tzinfo=pytz.UTC),
            datetime(2023, 5, 31, 14, 48, 28, 961000, tzinfo=pytz.UTC),
            datetime(2023, 5, 31, 14, 48, 29, 979000, tzinfo=pytz.UTC)
        ]

    @mark.unit
    def test_get_bool(self, minimal_preview_metadata: PreviewMetadataV063,
                      mminimal_preview_frame_timestamps: list[datetime]):
        expected_bools = [None, None, None, None, None, None, True]
        expected_result = [FrameSignal(utc_time=dt, value=val) for dt, val in zip(mminimal_preview_frame_timestamps,
                                                                                  expected_bools)]

        result = list(minimal_preview_metadata.get_bool("artificial_field"))
        assert expected_result == result

    def test_get_float(self, minimal_preview_metadata: PreviewMetadataV063,
                       mminimal_preview_frame_timestamps: list[datetime]):

        expected_floats = [0.706, 0.7, 0.697, 0.7, 0.703, 0.697, 0.697]
        expected_result = [
            FrameSignal(
                utc_time=dt, value=val) for dt, val in zip(
                mminimal_preview_frame_timestamps, expected_floats)]

        result = list(minimal_preview_metadata.get_float("CameraVerticalShifted"))
        for x in result:
            print(x.utc_time)
        assert expected_result == result

    def test_get_string(self, minimal_preview_metadata: PreviewMetadataV063,
                        mminimal_preview_frame_timestamps: list[datetime]):
        expected_string = ["7NBRHCX1KM", None, None, None, None, None, None]
        expected_result = [FrameSignal(utc_time=dt, value=val) for dt, val in zip(mminimal_preview_frame_timestamps,
                                                                                  expected_string)]

        result = list(minimal_preview_metadata.get_string("RideInfo_ride_id"))
        assert expected_result == result

    def test_get_int(self, minimal_preview_metadata: PreviewMetadataV063,
                     mminimal_preview_frame_timestamps: list[datetime]):

        expected_int = [158218, None, None, None, None, None, None]
        expected_result = [FrameSignal(utc_time=dt, value=val) for dt, val in zip(mminimal_preview_frame_timestamps,
                                                                                  expected_int)]

        result = list(minimal_preview_metadata.get_integer("RideInfo_local_startime_ms"))
        assert expected_result == result
