"""API service module."""
import logging
import re
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from metadata.api.db import Persistence

_logger = logging.getLogger("metadata_api." + __name__)
RELEVANT_DEVICE_SIGNALS = {
    "interior_camera_health_response_cvb",
    "interior_camera_health_response_cve",
    "CameraViewBlocked",
    "CameraVerticalShifted",
    "interior_camera_health_response_audio_blocked",
    "interior_camera_health_response_audio_distorted",
    "interior_camera_health_response_audio_signal",
    "PersonCount_value",
    "DrivingStatus",
    "DoorClosedConfidence",
    "Gnss_satellites_used",
    "Gnss_horizontal_speed",
    "Gnss_horizontal_speed_accuracy",
    "rms_ch0",
    "rms_ch1",
    "sum_ch0",
    "sum_ch1",
    "RideInfo_people_count_before_value",
    "RideInfo_people_count_after_value",
    "Snapshots"
}

MONGODB_PIPELINE_PREFIX_ADD_START_AT_END_AT = [
    {
        "$addFields": {
            "start_at": {
                "$convert": {
                    "input": {"$arrayElemAt": [{"$split": ["$video_id", "_"]}, -2]},
                    "to": "long",
                    "onError": 0,
                    "onNull": 0
                }
            },
            "end_at": {
                "$convert": {
                    "input": {"$arrayElemAt": [{"$split": ["$video_id", "_"]}, -1]},
                    "to": "long",
                    "onError": 0,
                    "onNull": 0
                }
            }
        }
    }
]


class ApiService:
    """API service class."""

    def __init__(self, database: Persistence, s3_client):
        self.__db = database
        self.__s3 = s3_client

    def get_video_signals(self, video_id: str):  # pylint: disable=too-many-locals
        """
        Gets the signals associated with a video

        Args:
            video_id (str): id of the video to fetch the signals form

        Raises:
            LookupError: if the video is not found

        Returns:
            signals (obj): signals for given video_id
        """
        signals = {}

        recording_item = self.__db.get_signals(video_id)

        for signal_group in recording_item["signals"]:
            if (signal_group["source"] in {"MDF", "MDFParser"}):
                signals[signal_group["source"]
                        ] = self.__create_video_signals_object(signal_group)
            else:
                signals[signal_group["algo_out_id"].split(
                    "_")[-1]] = self.__create_video_signals_object(signal_group)
        _logger.info("%s got signal fields %s", video_id, signals.keys())
        return signals

    def __create_video_signals_object(self, chc_result, time_offset: timedelta = timedelta(seconds=0)):
        def __convert_string_into_timedelta(timestamp_str) -> timedelta:
            # We have two formats for timedeltas: HH:MM:SS:MS and HH:MM:SS.MS
            time_obj = timestamp_str.replace(".", ":").split(":")
            timedelta_input = {"hours": 0, "minutes": 0, "seconds": 0, "microseconds": 0}
            for index, key in enumerate(timedelta_input):
                if index >= len(time_obj):
                    break
                timedelta_input[key] = int(time_obj[index])

            return timedelta(**timedelta_input)

        result_signals = {}
        if chc_result["signals"] and len(
                chc_result["signals"]) > 0 and isinstance(
                list(
                chc_result["signals"].values())[0],
                dict):

            for timestamp, signals in chc_result["signals"].items():
                timestamp_with_offset = str(__convert_string_into_timedelta(timestamp) + time_offset)
                result_signals[timestamp_with_offset] = {
                    key: signals[key] for key in RELEVANT_DEVICE_SIGNALS if key in signals}

        elif chc_result["signals"] and type(chc_result["signals"]):
            for key, value in chc_result["signals"].items():
                result_signals[key] = {}
                result_signals[key]["CameraViewBlocked"] = value

        return result_signals

    def update_video_description(self, video_id, description):
        """Calls the db module method to update a recording description"""
        self.__db.update_recording_description(video_id, description)

    def create_anonymized_video_url(self, recording_id):
        """
        Creates a presigned url for an anonymized video in S3. The location
        of the anonymized video for a given recording is fetched from the
        database

        Args:
            recording_id (str): id of the original recording

        Returns:
            url (str): presigned url for anonymized video
        """
        url = None
        entry = self.__db.get_algo_output("Anonymize", recording_id)
        if entry:
            # Get video path and split it into bucket and key
            s3_path = entry["output_paths"]["video"]
            bucket, path = s3_path.split("/", 1)
            url = self.__create_video_url(bucket, path)
        return url

    def create_video_url(self, bucket, folder, file):
        """
        Creates a presigned url for any video in S3, given its S3 bucket,
        folder and file name

        Args:
            bucket (str): S3 bucket where the video is
            folder (str): S3 folder where the video is
            file (str): filename of the video

        Returns:
            url (str): presigned url for the video
        """
        path = Path(folder) / Path(file)
        return self.__create_video_url(bucket, str(path))

    def __create_video_url(self, bucket, path):
        """
        Creates a presigned url for a video in S3 from the S3 client

        Args:
            bucket (str): s3 bucket where the video is
            path (str): path within the bucket where the file is

        Returns:
            url (str): presigned url for the video
        """
        params_s3 = {"Bucket": bucket, "Key": path}
        url = self.__s3.generate_presigned_url("get_object",
                                               Params=params_s3)
        return url

    def get_table_data(self, page_size: int, page: int,  # pylint: disable=too-many-arguments
                       query: Optional[List[dict]],
                       operator: Optional[str],
                       sorting: Optional[str],
                       direction: Optional[str]) -> Tuple[List[Dict], int, int]:
        """Gets table data from the database based on input parameters.
        Args:
            page_size (int): size of the page to be returned from the db.
            page (int): page number.
            query (Optional[List[dict]]): self-defined formatted for querying data in mongodb.
            operator (Optional[str]): aggregation association operators (AND, OR).
            sorting (Optional[str]): field used to sort the result set.
            direction (Optional[str]): asc or desc for ascending or descending order.

        Returns:
            returning a tuple containing a list of dictionaries, the total count of recordings and the number of pages
        """
        additional_query = None
        if query and query[0] != {} and operator:
            additional_query = self.__parse_query(query, operator)
        sorting_query = None
        if sorting and direction:
            sorting_query = self.__parse_sorting(sorting, direction)

        _logger.info("additional_query %s: ", additional_query)
        _logger.info("sorting_query %s: ", sorting_query)

        recordings, number_recordings, number_pages = self.__db.get_recording_list(
            page_size, page, additional_query, sorting_query)
        table_data = [self.__map_recording_object(r) for r in recordings]

        _logger.info("number_recordings %s:\n", number_recordings)

        return table_data, number_recordings, number_pages

    # State all valid query fields and their corresponding database field path
    __query_fields = {
        "_id": "video_id",
        "processing_list": "pipeline_execution.processing_list",
        "snapshots": "recording_overview.#snapshots",
        "data_status": "pipeline_execution.data_status",
        "last_updated": "pipeline_execution.last_updated",
        "length": "recording_overview.length",
        "time": "recording_overview.time",
        "resolution": "resolution",
        "number_chc_events": "recording_overview.number_chc_events",
        "lengthCHC": "recording_overview.chc_duration",
        "gnss_coverage": "recording_overview.gnss_coverage",
        "max_audio_loudness": "recording_overview.max_audio_loudness",
        "max_person_count": "recording_overview.max_person_count",
        "mean_audio_bias": "recording_overview.mean_audio_bias",
        "median_person_count": "recording_overview.median_person_count",
        "variance_person_count": "recording_overview.variance_person_count",
        "ride_detection_people_count_before": "recording_overview.ride_detection_people_count_before",
        "ride_detection_people_count_after": "recording_overview.ride_detection_people_count_after",
        "sum_door_closed": "recording_overview.sum_door_closed",
        "deviceID": "recording_overview.deviceID",
        "tenantID": "recording_overview.tenantID"
    }

    def __parse_query(self, query, logic_operator):
        # State all valid logical operators and their corresponding mongo query
        logic_operators = {
            "or": "$or",
            "and": "$and"
        }

        # State all valid query operators and their corresponding ongo operators
        # (used for validation and later conversion)
        operators = {
            "==": "$eq",
            ">": "$gt",
            "<": "$lt",
            "!=": "$ne",
            "has": "$regex"
        }

        # Check if operator is valid
        assert logic_operator.lower() in logic_operators, "Invalid/Forbidden logical operator"

        # Check if all query fields are valid
        for subquery in query:
            assert [fieldname for fieldname in subquery if fieldname not in self.__query_fields] == [
            ], "Invalid/Forbidden query keys"

        # Create empty list that will contain all sub queries received
        query_list = []

        for query_item in query:
            for fieldname, field_query in query_item.items():
                operator = list(field_query.keys())[0]
                operation_value = field_query[operator]

                # OPERATOR VALIDATION + CONVERSION
                # Check if operator is valid
                assert operator in operators, "Invalid/Forbidden query operators"

                # Convert the operator to mongo syntax
                mongo_operator = operators[operator]

                # VALUE VALIDATION
                # Check if value is valid (i.e. alphanumeric and/or with characters _ : . -)
                # NOTE: spaces are allowed in the value string
                assert re.findall(
                    r"^[a-zA-Z0-9_:.\s-]*$", str(operation_value)) != [], "Invalid/Forbidden query values"

                # Create subquery.
                # NOTE: $exists -> used to make sure items without
                # the parameter set in key are not also returned
                subquery = {self.__query_fields[fieldname]: {
                    "$exists": "true", mongo_operator: operation_value}}

                # Append subquery to list
                query_list.append(subquery)

        # Append selected logical operator to query
        # NOTE: Resulting format -> { <AND/OR>: [<subquery1>, <subquery2>, ...] },
        #       which translates into: <subquery1> AND/OR <subquery2> AND/OR ...
        return {logic_operators[logic_operator.lower()]: query_list}

    def __parse_sorting(self, sorting: str, direction: str) -> Dict[str, int]:
        directions = {
            "asc": 1,
            "dsc": -1
        }

        # Check if operator is valid
        assert direction.lower() in directions, "Invalid/Forbidden sorting direction"

        # Check if the sorting field is valid
        assert sorting in self.__query_fields, "Invalid/Forbidden sorting field"

        return {self.__query_fields[sorting]: directions[direction]}

    def get_media_entry(self, recording_id):
        """
        Gets media entry from database and produces a mapped version of it

        Args:
            recording_id (str): id of the recording

        Returns:
            result (obj): mapped recording object
        """
        recording_item = self.__db.get_media_entry(recording_id)
        result = self.__map_recording_object(recording_item)
        return result

    def get_single_recording(self, recording_id):
        """
        Gets a single recording

        Args:
            recording_id (str): recording item identifier

        Returns:
            result (obj): recording object
        """
        recording_item = self.__db.get_single_recording(recording_id)
        result = self.__map_recording_object(recording_item)

        # add LQ video info if neccessary
        lq_video = self.__check_and_get_lq_video_info(recording_id)
        if lq_video:
            result["lq_video"] = lq_video
        return result

    def __map_recording_object(self, recording_item):
        """
        Produces a mapped recording object from a given recording item

        Args:
            recording_item (obj): recording item

        Returns:
            result (obj): mapped recording object
        """
        recording_object = {}

        recording_overview = recording_item.get("recording_overview", {})
        recording_object["tenant"] = recording_overview.get("tenantID", "-")
        # the front-end cannot access the field if we set as "#snapshots"
        recording_object["snapshots"] = recording_overview.get(
            "#snapshots", "-")
        # the front-end cannot access the field if we set as "#snapshots"
        recording_object["#snapshots"] = recording_overview.get(
            "#snapshots", "-")
        recording_object["snapshots_paths"] = recording_overview.get(
            "snapshots_paths", "")
        recording_object["length"] = recording_overview.get("length", "-")
        recording_object["time"] = recording_overview.get("time", "-")
        recording_object["deviceID"] = recording_overview.get("deviceID", "-")
        recording_object["description"] = recording_overview.get("description")
        recording_object["_id"] = recording_item.get("video_id")
        recording_object["resolution"] = recording_item.get("resolution", "-")
        recording_object["number_chc_events"] = recording_overview.get(
            "number_chc_events", "-")
        recording_object["lengthCHC"] = recording_overview.get(
            "chc_duration", "-")
        recording_object["gnss_coverage"] = recording_overview.get(
            "gnss_coverage", "-")
        recording_object["max_audio_loudness"] = recording_overview.get(
            "max_audio_loudness", "-")
        recording_object["max_person_count"] = recording_overview.get(
            "max_person_count", "-")
        recording_object["mean_audio_bias"] = recording_overview.get(
            "mean_audio_bias", "-")
        recording_object["median_person_count"] = recording_overview.get(
            "median_person_count", "-")
        recording_object["variance_person_count"] = recording_overview.get(
            "variance_person_count", "-")
        recording_object["ride_detection_people_count_before"] = recording_overview.get(
            "ride_detection_people_count_before", "-")
        recording_object["ride_detection_people_count_after"] = recording_overview.get(
            "ride_detection_people_count_after", "-")
        recording_object["sum_door_closed"] = recording_overview.get(
            "sum_door_closed", "-")
        pipeline_execution = recording_item.get("pipeline_execution", {})
        recording_object["processing_list"] = pipeline_execution.get(
            "processing_list", "-")
        recording_object["data_status"] = pipeline_execution.get(
            "data_status", "-")
        recording_object["last_updated"] = pipeline_execution.get(
            "last_updated", "2020-01-01T00:00:00.1Z").split(".", 1)[0].replace("T", " ")

        return recording_object

    def __check_and_get_lq_video_info(self, entry_id):
        recorder_name_matcher = re.match(r".+_([^_]+)_\d+_\d+", entry_id)
        if not recorder_name_matcher or len(recorder_name_matcher.groups()) != 1:
            _logger.warning(
                "Could not parse recorder information from %s", entry_id)
            return None

        if recorder_name_matcher.group(1) != "TrainingRecorder":
            _logger.debug(
                "Skipping LQ video search for %s because it is recorded with %s",
                entry_id,
                recorder_name_matcher.group(1))
            return None
        lq_id = entry_id.replace("TrainingRecorder", "InteriorRecorder")
        try:
            lq_entry = self.__db.get_single_recording(lq_id)
        except BaseException:  # pylint: disable=broad-except
            return None
        lq_video_details = lq_entry.get("recording_overview", {})

        lq_video = {}
        lq_video["id"] = lq_id
        if "length" in lq_video_details:
            lq_video["length"] = lq_video_details["length"]
        if "time" in lq_video_details:
            lq_video["time"] = lq_video_details["time"]
        if "resolution" in lq_entry:
            lq_video["resolution"] = lq_entry["resolution"]
        if "#snapshots" in lq_video_details:
            lq_video["snapshots"] = lq_video_details["#snapshots"]

        return lq_video
