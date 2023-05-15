# type: ignore
""" metacontent ingestor tests """
from datetime import datetime
from typing import Callable
from unittest.mock import ANY, Mock, call, patch

import pytest
import pytz

from base.aws.container_services import ContainerServices
from base.model.artifacts import (MetadataArtifact, MetadataType, RecorderType,
                                  SignalsArtifact, TimeWindow, VideoArtifact)
from sdretriever.ingestor.metacontent import (MetacontentChunk,
                                              MetacontentDevCloud,
                                              MetacontentIngestor)
from sdretriever.ingestor.video_metadata import VideoMetadataIngestor

RCC_BUCKET = "test-rcc-bucket"


@pytest.fixture
def container_services() -> ContainerServices:
    container_services = Mock()
    type(container_services).rcc_info = {
        "s3_bucket": RCC_BUCKET,
    }
    return container_services


@pytest.fixture
def s3_client_factory_fix():
    s3_client = Mock()

    def s3_client_factory():
        return s3_client
    return s3_client_factory


@pytest.fixture
def s3_controller():
    s3_controller = Mock()
    s3_controller.upload_file = Mock()
    return s3_controller


@pytest.fixture
def s3_finder():
    s3_finder = Mock()
    return s3_finder


@pytest.fixture
def metacontent_ingestor(container_services: Mock,
                         s3_client_factory_fix: Callable,
                         s3_controller: Mock,
                         s3_finder: Mock) -> MetacontentIngestor:
    return VideoMetadataIngestor(
        container_services=container_services,
        rcc_s3_client_factory=s3_client_factory_fix,
        s3_controller=s3_controller,
        s3_finder=s3_finder
    )


@pytest.mark.unit
@pytest.mark.parametrize("input_size,expected,total", [
    (
        [
            MetacontentChunk(data=b"1", filename="test-chunk1.json"),
        ],
        [
            1
        ],
        "1.0 Bytes"
    ),
    (
        [
            MetacontentChunk(data=b"1", filename="test-chunk1.json"),
            MetacontentChunk(data=b"2", filename="test-chunk2.json"),
        ],
        [
            512,
            512
        ],
        "1.0 KB"
    ),
    (
        [
            MetacontentChunk(data=b"1", filename="test-chunk1.json"),
            MetacontentChunk(data=b"2", filename="test-chunk2.json"),
            MetacontentChunk(data=b"3", filename="test-chunk3.json"),
        ],
        [
            512,
            512,
            256
        ],
        "1.2 KB"
    ),
    (
        [
            MetacontentChunk(data=b"1", filename="test-chunk1.json"),
            MetacontentChunk(data=b"2", filename="test-chunk2.json"),
        ],
        [
            1_048_576,
            524288
        ],
        "1.5 MB"
    ),
    (
        [
            MetacontentChunk(data=b"1", filename="test-chunk1.json"),
        ],
        [
            1_073_741_824
        ],
        "1.0 GB"
    ),
    (
        [
            MetacontentChunk(data=b"1", filename="test-chunk1.json"),
        ],
        [
            1_099_511_627_776
        ],
        "1.0 TB"
    ),
    (
        [
            MetacontentChunk(data=b"1", filename="test-chunk1.json"),
            MetacontentChunk(data=b"2", filename="test-chunk2.json"),
            MetacontentChunk(data=b"3", filename="test-chunk3.json"),
        ],
        [
            1_099_511_627_776,
            1_099_511_627_776,
            1_099_511_627_776,
        ],
        "3.0 TB"
    ),
])
@patch("sdretriever.ingestor.metacontent.sys.getsizeof")
def test_get_readable_size_object(sizeof_patch: Mock,
                                  metacontent_ingestor: MetacontentIngestor,
                                  input_size: list[MetacontentChunk],
                                  expected: list[int],
                                  total: str):
    sizeof_patch.side_effect = expected
    result = metacontent_ingestor._get_readable_size_object(input_size)
    assert result == total


@pytest.mark.unit
@pytest.mark.parametrize("file", [
    (
        MetacontentDevCloud(
            bucket="test-bucket",
            data=b"test-data",
            extension=".json",
            msp="test-msp/",
            video_id="test-video-id",
        )
    ),
    (
        MetacontentDevCloud(
            bucket="test-bucket2",
            data=b"test-data2",
            extension=".json.zip",
            msp="test-msp2",
            video_id="test-video-id2",
        )
    )
])
def test_upload_metacontent_to_devcloud(metacontent_ingestor: MetacontentIngestor,
                                        file: MetacontentDevCloud,
                                        s3_controller: Mock):
    result = metacontent_ingestor._upload_metacontent_to_devcloud(file)
    s3_path = file.msp + file.video_id + file.extension
    s3_controller.upload_file.assert_called_once_with(file.data, file.bucket, s3_path)
    assert result == f"s3://{file.bucket}/{s3_path}"


STREAM1 = "InteriorRecorder-6433c789-08ee-421d-b2b3-7fb99ee0e947"
STREAM2 = "TrainingRecorder-553a2554-b0d4-4c08-85c6-a9dc1d013e41"
MONTH = 5
DAY = 7
YEAR = 2023
HOUR = 12

TENANT = "tenant1"
DEVICE = "device1"

video_artifact1 = VideoArtifact(
    stream_name=STREAM1,
    device_id=DEVICE,
    tenant_id=TENANT,
    recorder=RecorderType.INTERIOR,
    timestamp=datetime.now(tz=pytz.UTC),
    end_timestamp=datetime.now(tz=pytz.UTC),
    upload_timing=TimeWindow(datetime.now(tz=pytz.UTC), datetime.now(tz=pytz.UTC))
)

video_artifact2 = VideoArtifact(
    stream_name=STREAM2,
    device_id=DEVICE,
    tenant_id=TENANT,
    recorder=RecorderType.TRAINING,
    timestamp=datetime.now(tz=pytz.UTC),
    end_timestamp=datetime.now(tz=pytz.UTC),
    upload_timing=TimeWindow(datetime.now(tz=pytz.UTC), datetime.now(tz=pytz.UTC))
)

signals_artifact1 = SignalsArtifact(
    device_id=DEVICE,
    tenant_id=TENANT,
    referred_artifact=video_artifact1
)

signals_artifact2 = SignalsArtifact(
    device_id=DEVICE,
    tenant_id=TENANT,
    referred_artifact=video_artifact2
)


@pytest.mark.unit
@pytest.mark.parametrize("artifact,start_time,end_time,discover_result", [
    (
        signals_artifact1,
        datetime(YEAR, MONTH, DAY, HOUR, 0, 0, 0, tzinfo=pytz.UTC),
        datetime(YEAR, MONTH, DAY, HOUR + 2, 0, 0, 0, tzinfo=pytz.UTC),
        [
            f"{TENANT}/{DEVICE}/year={YEAR}/month={MONTH}/day={DAY}/hour={HOUR}/",
            f"{TENANT}/{DEVICE}/year={YEAR}/month={MONTH}/day={DAY}/hour={HOUR+1}/",
            f"{TENANT}/{DEVICE}/year={YEAR}/month={MONTH}/day={DAY}/hour={HOUR+2}/"
        ]
    ),
    (
        signals_artifact2,
        datetime(YEAR, MONTH, DAY, HOUR, 0, 0, 0, tzinfo=pytz.UTC),
        datetime(YEAR, MONTH, DAY, HOUR + 1, 0, 0, 0, tzinfo=pytz.UTC),
        [
            f"{TENANT}/{DEVICE}/year={YEAR}/month={MONTH}/day={DAY}/hour={HOUR}/",
            f"{TENANT}/{DEVICE}/year={YEAR}/month={MONTH}/day={DAY}/hour={HOUR+1}/",
        ]
    )
])
def test_get_chunks_lookup_paths(
        artifact: MetadataArtifact,
        start_time: datetime,
        end_time: datetime,
        discover_result: list[str],
        metacontent_ingestor: MetacontentIngestor,
        s3_finder: Mock):

    s3_finder.discover_s3_subfolders.return_value = discover_result

    results = metacontent_ingestor._get_chunks_lookup_paths(artifact, start_time, end_time)
    results_list = list(results)
    assert len(results_list) == len(discover_result)
    for i, result in enumerate(list(results_list)):
        assert result == f"{discover_result[i]}{artifact.referred_artifact.recorder.value}_{artifact.referred_artifact.recorder.value}"


@pytest.mark.unit
@patch("sdretriever.ingestor.metacontent.gzip.decompress")
@pytest.mark.parametrize("filepaths,contents", [
    (
        [],
        []
    ),
    (
        [
            "filepath1.zip",
            "filepath2"
        ],
        [
            b"content1",
            b"content2"
        ]
    ),
    (
        [
            "filepath1.zip",
            "filepath2.zip",
            "filepath3.zip"
        ],
        [
            b"content1",
            b"content2",
            b"content3"
        ]
    )
])
def test_get_metacontent_chunks(gzip_mock: Mock,
                                filepaths: list[str],
                                contents: list[bytes],
                                metacontent_ingestor: MetacontentIngestor,
                                container_services: Mock):
    container_services.download_file.side_effect = contents
    chunks = metacontent_ingestor._get_metacontent_chunks(filepaths)
    container_services.download_file.assert_has_calls([call(ANY, RCC_BUCKET, file) for file in filepaths])
    gzip_mock.assert_has_calls([call(ANY) for file in filepaths if file.endswith(".zip")])
    assert len(chunks) == len(filepaths)


@pytest.mark.unit
def test_search_chunks_in_s3_path(metacontent_ingestor: MetacontentIngestor):
    resp_mock = {
        'Contents': [
            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-498375935_10.mp4',
                    'LastModified': datetime(year=2022, month=10, day=10, tzinfo=pytz.UTC)
             },
            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-498375935_10.mp4_stream.json',
                    'LastModified': datetime(year=2022, month=10, day=10, tzinfo=pytz.UTC)
             },
            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-afdgdffde_20.mp4',
                    'LastModified': datetime(year=2022, month=10, day=10, tzinfo=pytz.UTC)
             },
            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-fgf12325e_10.mp4',
                    'LastModified': datetime(year=2022, month=10, day=10, tzinfo=pytz.UTC)
             },
            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-fgf12325e_10.mp4_stream.json.zip',
                    'LastModified': datetime(year=2022, month=10, day=10, tzinfo=pytz.UTC)
             },
            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-abc12325e_11.mp4.something.zip',
                    'LastModified': datetime(year=2022, month=10, day=10, tzinfo=pytz.UTC)
             },
            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-abc12325e_11.mp4',
                    'LastModified': datetime(year=2022, month=10, day=10, tzinfo=pytz.UTC)
             },
            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-456bfdbg.mp4',
             'LastModified': datetime(year=2022, month=10, day=10, tzinfo=pytz.UTC)
             },
            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TrainingRecorder_TrainingRecorder-456bfdbg_10.mp4',
             'LastModified': datetime(year=2022, month=10, day=10, tzinfo=pytz.UTC)
             }
        ]
    }

    metadata_expected = {
        'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-498375935_10.mp4_stream.json',
        'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-fgf12325e_10.mp4_stream.json.zip',
        'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-abc12325e_11.mp4.something.zip'}
    video_expected = {
        'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-498375935_10.mp4',
        'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-afdgdffde_20.mp4',
        'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-fgf12325e_10.mp4',
        'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-abc12325e_11.mp4'}
    reference_path = 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/'
    bucket = "BUCKET"
    metacontent_ingestor.check_if_s3_rcc_path_exists = Mock(return_value=(True, resp_mock))

    metadata_chunks_set, video_chunks_set = metacontent_ingestor._search_chunks_in_s3_path(
        reference_path, bucket, [".json", ".zip"], recorder_type="InteriorRecorder")

    assert metadata_expected == metadata_chunks_set
    assert video_expected == video_chunks_set
    metacontent_ingestor.check_if_s3_rcc_path_exists.assert_called_once_with(
        reference_path, bucket, max_s3_api_calls=5)


@pytest.mark.unit
def test_search_chunks_in_s3_path_time_bound(metacontent_ingestor):
    resp_mock = {
        'Contents': [
            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-498375935_10.mp4',
                    'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=59, tzinfo=pytz.UTC)
             },
            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-498375935_10.mp4_stream.json.zip',
                    'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1, tzinfo=pytz.UTC)
             },
            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-afdgdffde_10.mp4',
                    'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1, tzinfo=pytz.UTC)
             },
            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-fgf12325e_12.mp4',
                    'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1, tzinfo=pytz.UTC)
             },
            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-fgf12325e_12.mp4_stream.json.zip',
                    'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1, tzinfo=pytz.UTC)
             },
            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-abc12325e_15.mp4_stream.json.zip',
                    'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1, tzinfo=pytz.UTC)
             },
            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-abc12325e_15.mp4',
                    'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1, tzinfo=pytz.UTC)
             },

            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-a5464564.mp4.stream.imu.zip',
                    'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1, tzinfo=pytz.UTC)
             },
            {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-sdfsdds5e.mp4',
             'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1, tzinfo=pytz.UTC)
             }
        ]
    }

    metadata_expected = {
        'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-498375935_10.mp4_stream.json.zip',
        'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-fgf12325e_12.mp4_stream.json.zip',
        'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-abc12325e_15.mp4_stream.json.zip'}

    video_expected = {
        'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-afdgdffde_10.mp4',
        'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-fgf12325e_12.mp4',
        'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-abc12325e_15.mp4'}
    reference_path = 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/'
    bucket = "BUCKET"
    metacontent_ingestor.check_if_s3_rcc_path_exists = Mock(return_value=(True, resp_mock))

    start_time = datetime(year=2022, month=9, day=30, hour=20, minute=0, tzinfo=pytz.UTC)
    end_time = datetime(year=2022, month=9, day=30, hour=20, minute=30, tzinfo=pytz.UTC)

    metadata_chunks_set, video_chunks_set = metacontent_ingestor._search_chunks_in_s3_path(
        reference_path, bucket, [".json.zip"], start_time=start_time, end_time=end_time)
    assert metadata_expected == metadata_chunks_set
    assert video_expected == video_chunks_set
    metacontent_ingestor.check_if_s3_rcc_path_exists.assert_called_once_with(
        reference_path, bucket, max_s3_api_calls=5)


CHUNK_LOOKUP_PATHS = [
    "tenant1/device1/year=2021/month=05/day=07/hour=12/",
    "tenant1/device1/year=2021/month=05/day=07/hour=13/"
]
MP4_CHUNKS_1 = [
    "tenant1/device1/year=2021/month=05/day=07/hour=12/File1.mp4",
    "tenant1/device1/year=2021/month=05/day=07/hour=12/File2.mp4"
]
MP4_CHUNKS_2 = [
    "tenant1/device1/year=2021/month=05/day=07/hour=13/File3.mp4"
]
MDF_CHUNKS_1 = [
    "tenant1/device1/year=2021/month=05/day=07/hour=12/File1.mp4.metadata.json",
    "tenant1/device1/year=2021/month=05/day=07/hour=12/File2.mp4.metadata.json"
]
MDF_CHUNKS_2 = [
    "tenant1/device1/year=2021/month=05/day=07/hour=13/File3.mp4.metadata.json"
]


@pytest.mark.unit
def test_check_allparts_exist_successful(
        metacontent_ingestor: MetacontentIngestor,
        container_services: ContainerServices):
    # GIVEN
    metacontent_ingestor._get_file_extension = Mock(return_value=".json")  # type: ignore[method-assign]
    container_services.check_if_tenant_and_deviceid_exists_and_log_on_error = Mock(  # type: ignore[method-assign]
        return_value=True)
    metacontent_ingestor._get_chunks_lookup_paths = Mock(return_value=CHUNK_LOOKUP_PATHS)  # type: ignore[method-assign]
    metacontent_ingestor._search_chunks_in_s3_path = Mock(  # type: ignore[method-assign]
        side_effect=[(MDF_CHUNKS_1, MP4_CHUNKS_1), (MDF_CHUNKS_2, MP4_CHUNKS_2)])

    # WHEN
    result, chunks = metacontent_ingestor._check_allparts_exist(signals_artifact1)

    # THEN
    assert result
    assert len(chunks) == 3
    assert chunks == {*MDF_CHUNKS_1, *MDF_CHUNKS_2}


@pytest.mark.unit
def test_check_allparts_exist_failed_no_video_chunks(
        metacontent_ingestor: MetacontentIngestor,
        container_services: ContainerServices):
    # GIVEN
    metacontent_ingestor._get_file_extension = Mock(return_value=".json")  # type: ignore[method-assign]
    container_services.check_if_tenant_and_deviceid_exists_and_log_on_error = Mock(  # type: ignore[method-assign]
        return_value=True)
    metacontent_ingestor._get_chunks_lookup_paths = Mock(return_value=CHUNK_LOOKUP_PATHS)  # type: ignore[method-assign]
    metacontent_ingestor._search_chunks_in_s3_path = Mock(  # type: ignore[method-assign]
        side_effect=[(MDF_CHUNKS_1, []), (MDF_CHUNKS_2, [])])

    # WHEN
    result, chunks = metacontent_ingestor._check_allparts_exist(signals_artifact1)

    # THEN
    assert result == False
    assert len(chunks) == 0


@pytest.mark.unit
def test_check_allparts_exist_queries_until_today_if_neccessary(
        metacontent_ingestor: MetacontentIngestor,
        container_services: ContainerServices):
    # GIVEN
    metacontent_ingestor._get_file_extension = Mock(return_value=".json")  # type: ignore[method-assign]
    container_services.check_if_tenant_and_deviceid_exists_and_log_on_error = Mock(  # type: ignore[method-assign]
        return_value=True)
    metacontent_ingestor._get_chunks_lookup_paths = Mock(return_value=CHUNK_LOOKUP_PATHS)  # type: ignore[method-assign]
    metacontent_ingestor._search_chunks_in_s3_path = Mock(  # type: ignore[method-assign]
        side_effect=[(MDF_CHUNKS_1, MP4_CHUNKS_1), ([], MP4_CHUNKS_2), (MDF_CHUNKS_2, MP4_CHUNKS_2)])

    # WHEN
    result, chunks = metacontent_ingestor._check_allparts_exist(signals_artifact1)

    # THEN
    assert result
    assert len(chunks) == 3
    assert chunks == {*MDF_CHUNKS_1, *MDF_CHUNKS_2}
    print(metacontent_ingestor._get_chunks_lookup_paths.call_args[1])
    lookup_end_date: datetime = metacontent_ingestor._get_chunks_lookup_paths.call_args[1]['end_time']
    assert lookup_end_date.date() == datetime.now().date()
