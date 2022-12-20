
import gzip
import hashlib
import json
import logging as log
import os
import re
import subprocess  # nosec
import tempfile
from abc import abstractmethod
from calendar import monthrange
from copy import copy
from datetime import datetime, timedelta
from operator import itemgetter
from pathlib import Path
from typing import Iterator, Optional, Tuple, TypeVar
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client
from sdretriever.message import VideoMessage

from base.aws.container_services import ContainerServices, RCCS3ObjectParams
from base.aws.shared_functions import StsHelper
from base.timestamps import from_epoch_seconds_or_milliseconds

# file format for metadata stored on DevCloud raw S3
METADATA_FILE_EXT = '_metadata_full.json'
ST = TypeVar('ST', datetime, str, int)  # SnapshotTimestamp type
LOGGER = log.getLogger("SDRetriever." + __name__)


class Ingestor(object):

    def __init__(
            self,
            container_services: ContainerServices,
            s3_client: S3Client,
            sqs_client,
            sts_helper: StsHelper) -> None:
        self.CS = container_services
        self.S3_CLIENT = s3_client
        self.SQS_CLIENT = sqs_client
        self.STS_HELPER = sts_helper
        self._rcc_credentials = self.STS_HELPER.get_credentials()
        self.metadata_queue = self.CS.sqs_queues_list["Metadata"]

    @abstractmethod
    def ingest(self, message):
        """Ingests the artifacts described in a message into the DevCloud"""
        pass

    def check_if_s3_rcc_path_exists(self, s3_path: str, bucket: str, messageid: str = None,
                                    max_s3_api_calls: int = 1, exact=False) -> Tuple[bool, dict]:
        """Verify if path exists on target S3 bucket.

        - Check if the file exists in a given path.
        - Verifies if the object found size is bigger then 0.
        - If the object is not found, continue to look for tenant and deviceid prefixes to provide logging information
        - If the exact argument is true, all objects will still be retrieved but the first return value will only be true if one of
        the objects matches th path exactly.

        Args:
            s3_path (str): S3 path to look for.
            bucket (str): S3 bucket name.
            messageid (str): SQS message ID
            max_s3_api_calls (str): Maximum s3 list api calls to be made
            exact (bool): Only match files with exact name, if false it will only match the prefix
        Returns:
            A tuple containing a boolean response if the path was found and a dictionary with the S3 objects information
        """
        s3_object_params = RCCS3ObjectParams(s3_path=s3_path, bucket=bucket)
        s3_client = self.RCC_S3_CLIENT

        # Check if there is a file with the same name already stored on target S3 bucket
        try:
            list_objects_response: dict = dict(ContainerServices.list_s3_objects(
                s3_object_params.s3_path, bucket, s3_client, max_iterations=max_s3_api_calls))

            if list_objects_response.get('KeyCount') and int(list_objects_response['Contents'][0]['Size']) == 0:
                return False, list_objects_response

        except Exception:  # pylint: disable=broad-except
            return ContainerServices.check_if_tenant_and_deviceid_exists_and_log_on_error(
                s3_client, s3_object_params, messageid), {}

        if list_objects_response is None or list_objects_response.get('KeyCount', 0) == 0:
            return False, {}

        if exact:
            exact_match = s3_path in [object_dict['Key'] for object_dict in list_objects_response['Contents']]
            return exact_match, list_objects_response

        return True, list_objects_response

    @ property
    def RCC_S3_CLIENT(self):
        client = boto3.client('s3',
                              region_name='eu-central-1',
                              aws_access_key_id=self.accesskeyid,
                              aws_secret_access_key=self.secretaccesskey,
                              aws_session_token=self.sessiontoken)
        return client

    @ property
    def accesskeyid(self):
        self._rcc_credentials = self.STS_HELPER.get_credentials()
        return self._rcc_credentials['AccessKeyId']

    @ property
    def secretaccesskey(self):
        self._rcc_credentials = self.STS_HELPER.get_credentials()
        return self._rcc_credentials['SecretAccessKey']

    @ property
    def sessiontoken(self):
        self._rcc_credentials = self.STS_HELPER.get_credentials()
        return self._rcc_credentials['SessionToken']


class VideoIngestor(Ingestor):
    def __init__(self, container_services, s3_client, sqs_client, sts_helper, frame_buffer=0) -> None:
        super().__init__(container_services, s3_client, sqs_client, sts_helper)
        self.clip_ext = ".mp4"
        self.STREAM_TIMESTAMP_TYPE = 'PRODUCER_TIMESTAMP'
        self.frame_buffer = frame_buffer

    @ staticmethod
    def _ffmpeg_probe_video(video_bytes) -> dict:
        # On completion of the context or destruction of the temporary directory object,
        # the newly created temporary directory and all its contents are removed from the filesystem.
        with tempfile.TemporaryDirectory() as auto_cleaned_up_dir:

            # Store bytes into current working directory as video
            temp_video_file = os.path.join(auto_cleaned_up_dir, 'input_video.mp4')
            with open(temp_video_file, "wb") as f:
                f.write(video_bytes)

            # Execute ffprobe command to get video clip info
            result = subprocess.run(["/usr/bin/ffprobe",  # nosec
                                    "-v",
                                     "error",
                                     "-show_format",
                                     "-show_streams",
                                     "-print_format",
                                     "json",
                                     temp_video_file],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT)

            # Load ffprobe output (bytes) as JSON
            decoded_info = (result.stdout).decode("utf-8")
            video_info = json.loads(decoded_info)
            return video_info

    def ingest(
            self,
            video_msg: VideoMessage,
            training_whitelist: list[str] = [],
            request_training_upload=True) -> Optional[dict]:
        '''Obtain video from KinesisVideoStreams and upload it to our raw data S3'''
        # Define the target path where to place the video
        if video_msg.streamname and "srxdriverpr" in video_msg.streamname:
            s3_folder = self.CS.sdr_folder['driver_pr']
        else:
            s3_folder = self.CS.sdr_folder['debug']

        # Get clip from KinesisVideoStream
        try:
            # Requests credentials to assume specific cross-account role on Kinesis
            role_credentials = self.STS_HELPER.get_credentials()
            video_from = from_epoch_seconds_or_milliseconds(
                video_msg.footagefrom)
            video_to = from_epoch_seconds_or_milliseconds(video_msg.footageto)
            seed = f"{video_msg.streamname}_{int(video_from.timestamp() * 1000)}_{int(video_to.timestamp() * 1000)}"
            internal_message_reference_id = hashlib.sha256(seed.encode("utf-8")).hexdigest()
            video_bytes, video_start_ts, video_end_ts = self.CS.get_kinesis_clip(
                role_credentials, video_msg.streamname, video_from, video_to, self.STREAM_TIMESTAMP_TYPE)
            video_start = round(video_start_ts.timestamp() * 1000)
            video_end = round(video_end_ts.timestamp() * 1000)
        except Exception as exception:
            LOGGER.exception(
                f"Could not obtain Kinesis clip from stream {video_msg.streamname} between {repr(video_from)} and {repr(video_to)} -> {repr(exception)}",
                exception,
                extra={
                    "messageid": video_msg.messageid})
            return None

        # Upload video clip into raw data S3 bucket
        s3_filename = f"{video_msg.streamname}_{video_start}_{video_end}"
        s3_path = s3_folder + s3_filename + self.clip_ext
        try:
            self.CS.upload_file(self.S3_CLIENT, video_bytes,
                                self.CS.raw_s3, s3_path)
            LOGGER.info(f"Successfully uploaded to {self.CS.raw_s3}/{s3_path}", extra={
                        "messageid": video_msg.messageid})
        except Exception as exception:
            LOGGER.error(f"Could not upload file to {s3_path} -> {repr(exception)}", extra={
                         "messageid": video_msg.messageid})
            return None

        '''Obtain video details via ffprobe and prepare data to be used to generate the video's entry on the database'''

        video_info = self._ffmpeg_probe_video(video_bytes)

        # Calculate video information
        width = str(video_info["streams"][0]["width"])
        height = str(video_info["streams"][0]["height"])
        video_seconds = round(float(video_info["format"]["duration"]))

        # Build the cedord data to be sent into DB
        db_record_data = {
            "_id": s3_filename,
            "MDF_available": "No",
            "media_type": "video",
            "s3_path": self.CS.raw_s3 + "/" + s3_path,
            "footagefrom": video_start,
            "footageto": video_end,
            "tenant": video_msg.tenant,
            "deviceid": video_msg.deviceid,
            'length': str(timedelta(seconds=video_seconds)),
            '#snapshots': str(0),
            'snapshots_paths': [],
            "sync_file_ext": "",
            'resolution': width + "x" + height,
            "internal_message_reference_id": internal_message_reference_id,
        }

        # Generate dictionary with info to send to Selector container for training data request (high quality )
        is_interior_recording = video_msg.video_recording_type() == 'InteriorRecorder'
        if is_interior_recording and request_training_upload and video_msg.tenant in training_whitelist:
            hq_request = {
                "streamName": f"{video_msg.tenant}_{video_msg.deviceid}_InteriorRecorder",
                "deviceId": video_msg.deviceid,
                "footageFrom": video_msg.footagefrom - self.frame_buffer,
                "footageTo": video_msg.footageto + self.frame_buffer
            }
            # Send message to secondary input queue of Selector container
            hq_selector_queue = self.CS.sqs_queues_list["HQ_Selector"]
            self.CS.send_message(
                self.SQS_CLIENT, hq_selector_queue, hq_request)

        return db_record_data


class SnapshotIngestor(Ingestor):
    def __init__(self, container_services, s3_client, sqs_client, sts_helper) -> None:
        super().__init__(container_services, s3_client, sqs_client, sts_helper)

    @ staticmethod
    def _snapshot_path_generator(tenant: str, device: str, start: ST, end: ST):
        """Generate the list of possible folders between the range of two timestamps

        Args:
            tenant (str): The device tenant
            device (str): The device identifier
            start (ST): The lower limit of the time range
            end (ST, optional): The upper limit of the time range. Defaults to datetime.now().

        Returns:
            [str]: List with all possible paths between timestamp bounds, sorted old to new.
        """
        if not tenant or not device or not start or not end:
            return []
        # cast to datetime format
        start_timestamp = datetime.fromtimestamp(
            start / 1000.0) if not isinstance(start, datetime) else start
        end_timestamp = datetime.fromtimestamp(
            end / 1000.0) if not isinstance(end, datetime) else end
        if int(start.timestamp()) < 0 or int(end.timestamp()) < 0:
            return []

        dt = start_timestamp
        times = []
        # for all hourly intervals between start_timestamp and end_timestamp
        while dt <= end_timestamp or (dt.hour == end_timestamp.hour):
            # append its respective path to the list
            year = dt.strftime("%Y")
            month = dt.strftime("%m")
            day = dt.strftime("%d")
            hour = dt.strftime("%H")
            times.append(
                f"{tenant}/{device}/year={year}/month={month}/day={day}/hour={hour}/")
            dt = dt + timedelta(hours=1)
        # and return it
        return times

    def ingest(self, snap_msg):
        flag_do_not_delete = False
        uploads = 0
        # For all snapshots mentioned within, identify its snapshot info and save
        # it - (current file name, timestamp to append)

        for chunk in snap_msg.chunks:

            # Our SRX device is in Portugal, 1h diff to AWS
            timestamp_10 = datetime.utcfromtimestamp(
                chunk.start_timestamp_ms / 1000.0)  # + td(hours=-1.0)
            if chunk.uuid.endswith(".jpeg"):
                RCC_S3_bucket = self.CS.rcc_info.get('s3_bucket')
                # Define its new name by adding the timestamp as a suffix
                uuid_no_format = chunk.uuid.rstrip(Path(chunk.uuid).suffix)
                snap_name = f"{snap_msg.tenant}_{snap_msg.deviceid}_{uuid_no_format}_{int(chunk.start_timestamp_ms)}.jpeg"
                exists_on_devcloud = ContainerServices.check_s3_file_exists(
                    self.S3_CLIENT, self.CS.raw_s3, 'Debug_Lync/' + snap_name)

                seed = snap_name.rstrip(Path(snap_name).suffix)
                internal_message_reference_id = hashlib.sha256(seed.encode("utf-8")).hexdigest()

                if not exists_on_devcloud:

                    # Determine where to search for the file on RCC S3
                    possible_locations = self._snapshot_path_generator(
                        snap_msg.tenant, snap_msg.deviceid, timestamp_10, datetime.now())

                    # For all those locations
                    for folder in possible_locations:
                        # If the file exists in this RCC folder
                        found_on_rcc, _ = self.check_if_s3_rcc_path_exists(
                            folder + chunk.uuid, RCC_S3_bucket, snap_msg.messageid, exact=True)

                        if found_on_rcc:
                            # Download jpeg from RCC
                            try:
                                snapshot_bytes = self.CS.download_file(
                                    self.RCC_S3_CLIENT, RCC_S3_bucket, folder + chunk.uuid)
                            except ClientError as e:
                                flag_do_not_delete = True
                                if e.response['Error']['Code'] == 'NoSuchKey':
                                    LOGGER.exception(
                                        f"Download failed because file was found but could not be downloaded on {RCC_S3_bucket}/{folder+chunk.uuid} - {e}. Will try again later.",
                                        extra={
                                            "messageid": snap_msg.messageid})
                                break

                            # Upload to DevCloud
                            self.CS.upload_file(
                                self.S3_CLIENT, snapshot_bytes, self.CS.raw_s3, 'Debug_Lync/' + snap_name)
                            LOGGER.info(f"Successfully uploaded to {self.CS.raw_s3}/{'Debug_Lync/'+snap_name}", extra={
                                        "messageid": snap_msg.messageid})

                            db_record_data = {
                                "_id": snap_name[:-5],
                                "s3_path": f"{self.CS.raw_s3}/Debug_Lync/{snap_name}",
                                "deviceid": snap_msg.deviceid,
                                "timestamp": chunk.start_timestamp_ms,
                                "tenant": snap_msg.tenant,
                                "media_type": "image",
                                "internal_message_reference_id": internal_message_reference_id,
                            }
                            self.CS.send_message(
                                self.SQS_CLIENT, self.metadata_queue, db_record_data)
                            LOGGER.info(f"Message sent to {self.metadata_queue} to create record for snapshot ", extra={
                                        "messageid": snap_msg.messageid})

                            uploads += 1
                            # Stop the search
                            break

                    if not found_on_rcc and not chunk.available:
                        LOGGER.info(
                            f"File {chunk.uuid} is not yet available in RCC S3, with upload status {chunk.upload_status}", extra={
                                "messageid": snap_msg.messageid})

                        flag_do_not_delete = True
                    elif not found_on_rcc and chunk.available:
                        LOGGER.info(
                            f"Could not find {chunk.uuid} in RCC S3, with upload status {chunk.upload_status}", extra={
                                "messageid": snap_msg.messageid})
                else:
                    LOGGER.info(f"File {'Debug_Lync/'+snap_name} already exists on {self.CS.raw_s3}", extra={
                                "messageid": snap_msg.messageid})
            else:
                LOGGER.info(f"Found something other than a snapshot: {chunk.uuid}", extra={
                            "messageid": snap_msg.messageid})
        LOGGER.info(f"Uploaded {uploads}/{len(snap_msg.chunks)} snapshots into {self.CS.raw_s3}",
                    extra={"messageid": snap_msg.messageid})
        return flag_do_not_delete


class MetadataIngestor(Ingestor):
    def __init__(self, container_services, s3_client, sqs_client, sts_helper) -> None:
        super().__init__(container_services, s3_client, sqs_client, sts_helper)

    @ staticmethod
    def _json_raise_on_duplicates(ordered_pairs):
        """Convert duplicate keys to JSON array or if JSON objects, merges them."""
        d = {}
        for (k, v) in ordered_pairs:
            if k in d:
                if isinstance(d[k], dict) and isinstance(v, dict):
                    for (sub_k, sub_v) in v.items():
                        d[k][sub_k] = sub_v
                elif isinstance(d[k], list):
                    d[k].append(v)
                else:
                    d[k] = [d[k], v]
            else:
                d[k] = v
        return d

    def _get_chunks_lookup_paths(
            self,
            message: VideoMessage,
            start_time: datetime = None,
            end_time: datetime = None) -> Iterator[str]:
        """
        Get all paths to search for chunks in RCC S3 bucket between two timestamps.
        It includes the start and end hour folder.

        Args:
            message (VideoMessage): _description_
            start_time (datetime, optional): The start date to search
                if not specified, defaults to time the video finished uploading.
            end_time (datetime, optional): The end date to stop search
                if not specified, defaults to time the video finished uploading.

        Yields:
            Iterator[str]: An Iterator containing all possible paths
        """
        # If end time is not provided use the message upload timestamps
        if end_time is None:
            end_time = message.uploadfinished

        if start_time is None:
            start_time = message.uploadstarted

        start_time = start_time.replace(microsecond=0, second=0, minute=0)

        path = f'{message.tenant}/{message.deviceid}/'

        paths = self._discover_s3_subfolders(
            path, self.CS.rcc_info["s3_bucket"], self.RCC_S3_CLIENT, start_time, end_time)

        for s3_path in paths:
            yield s3_path + f'{message.recording_type}_{message.recordingid}'

    def _discover_s3_subfolders(
            self,
            parent_folder: str,
            bucket: str,
            s3_client: S3Client,
            start_time: datetime,
            end_time: datetime) -> Iterator[str]:
        """
        Recursive function that will discover all RCC S3 subfolder between start_time and end_time.
        Return all the path available in S3 between start_time and end_time, including the start hour and end hour folder.

        Args:
            folder (str): Parent folder to search for. Needs to be inside a device folder.
            bucket (str): Bucket name.
            s3_client (S3Client): RCC S3 client.
            start_time (datetime): Time to start the search
            end_time (datetime): Time to end the search

        Returns:
            Iterator[str]: A list containing all paths from root to the last folder (hour folder).
        """

        LOGGER.debug(f'Discovering folders while searching on {start_time} - {end_time}')

        # Reset minutes
        start_time_zero = start_time.replace(minute=0)
        end_time_zero = end_time.replace(minute=0)

        list_objects_response = ContainerServices.list_s3_objects(
            parent_folder, bucket, s3_client, "/")

        sub_folders = []
        for content in list_objects_response['CommonPrefixes']:
            path = content['Prefix']
            year_groups = re.search("year=([0-9]{4})", path)
            month_groups = re.search("month=([0-9]{2})", path)
            day_groups = re.search("day=([0-9]{2})", path)
            hour_groups = re.search("hour=([0-9]{2})", path)

            current_time_start = copy(start_time_zero)
            current_time_end = copy(end_time_zero)

            # Cast the dates of the paths if they exist
            year = int(year_groups.groups()[0])
            month = int(month_groups.groups()[
                        0]) if month_groups is not None else None

            day = int(day_groups.groups()[0]
                      ) if day_groups is not None else None

            hour = int(hour_groups.groups()[
                       0]) if hour_groups is not None else None

            # kwargs use to replace date
            kwargs_replace = {}

            if year:
                kwargs_replace['year'] = year

            if month:
                kwargs_replace['month'] = month

            if day:
                kwargs_replace['day'] = day

            if hour:
                kwargs_replace['hour'] = hour

            if len(kwargs_replace) > 0:
                kwargs_replace_start = {
                    "year": start_time_zero.year,
                    "month": start_time_zero.month,
                    "day": start_time_zero.day,
                    "hour": start_time_zero.hour
                }
                kwargs_replace_start.update(kwargs_replace)

                # Make sure the month is valid
                max_day = monthrange(kwargs_replace_start["year"], kwargs_replace_start["month"])[1]
                if max_day < kwargs_replace_start["day"]:
                    kwargs_replace_start['day'] = max_day

                kwargs_replace_end = {
                    "year": end_time_zero.year,
                    "month": end_time_zero.month,
                    "day": end_time_zero.day,
                    "hour": end_time_zero.hour
                }
                kwargs_replace_end.update(kwargs_replace)

                # Make sure the month is valid
                max_day = monthrange(kwargs_replace_end["year"], kwargs_replace_end["month"])[1]
                if max_day < kwargs_replace_end["day"]:
                    kwargs_replace_end['day'] = max_day

                current_time_end = current_time_end.replace(**kwargs_replace_end)
                current_time_start = current_time_start.replace(**kwargs_replace_start)

            # Make sure the paths are within boundaries
            if current_time_start < start_time_zero or current_time_end > end_time_zero:
                continue

            # Additional check to avoid infinite loop
            if not path.endswith('/'):
                continue

            sub_folders.append(path)

        # Check if this is the latest stopping point
        if 'day=' in parent_folder:
            # Make sure all paths are completed and don't have missing folders
            # return [result for result in sub_folders if 'hour=' in result]

            for result in sub_folders:
                if 'hour=' in result:
                    yield result

            return

        # Call own function again for every result
        for folder in sub_folders:

            yield from self._discover_s3_subfolders(
                folder, bucket, s3_client, start_time, end_time)

    def _search_chunks_in_s3_path(self, s3_path: str, bucket: str, messageid: str,
                                  start_time: datetime = None, end_time: datetime = None) -> Tuple[set[str], set[str]]:
        """
        Lists all metadata and video chunks for a specific path.

        If start_time and end_time fields are provided then v chunks will
        only be fetch if they were modified between start_time and end_time.

        It searches up to a maximum of 5000 objects
        Args:
            s3_path (str): Path to list the chunks.
            bucket (str): The S3 bucket name.
            messageid (str): SQS message ID.
            start_time (datetime): Lower bound to fetch videos chunks
            end_time (datetime): Upper bound to fetch videos chunks

        Returns:
            Tuple[set[str], set[str]]: A tuple containing all metadata and video path chunks respectively.
        """
        has_files, resp = self.check_if_s3_rcc_path_exists(
            s3_path, bucket, messageid=messageid, max_s3_api_calls=5)

        metadata_chunks_set = set()
        video_chunks_set = set()

        if not has_files:
            return metadata_chunks_set, video_chunks_set

        for file_entry in resp['Contents']:
            file_path: str = file_entry['Key']
            modified_date: datetime = file_entry['LastModified'].replace(
                tzinfo=None)
            rexp = re.compile(r".+\.mp4.*(\.json|\.zip)$")

            if file_path.endswith('.mp4'):

                if start_time and modified_date < start_time:
                    LOGGER.debug(
                        "Ignoring chunk (%s) modified at (%s), it's under the uploadstarted datetime (%s)",
                        file_path,
                        str(modified_date),
                        str(end_time))
                    continue

                if end_time and modified_date > end_time:
                    LOGGER.debug(
                        "Ignoring chunk (%s) modified at (%s), it's over the uploadfinished datetime (%s)",
                        file_path,
                        str(modified_date),
                        str(end_time))
                    continue

                video_chunks_set.add(file_path)
                continue

            if rexp.match(file_path):
                metadata_chunks_set.add(file_path)

        return metadata_chunks_set, video_chunks_set

    def _search_for_match_chunks(self,
                                 lookup_paths: Iterator[str],
                                 mp4_chunks_left: set[str],
                                 bucket: str,
                                 messageid: str) -> tuple[bool,
                                                          set[str]]:
        """
        Search for metadata chunks on the provided paths and returns all metadata chunks found.
        The function will return as soon as all the matched chunks (compared to the video chunks) are found.

        Args:
            lookup_paths (Iterator[str]): Paths to search for on s3 bucket.
            mp4_chunks_left (set[str]): A set of the videos chunk ids (not the entire path) to be matched.
            bucket (str): The S3 bucket to search on.
            messageid (str): The message id.

        Returns:
            tuple[bool, set[str]]: A tuple with a boolean that is true if all metadata is found and a set containing the path for all metadata.
        """

        recording_regex = re.compile(r"/hour=[0-9]{2}/(.+\.mp4).*")
        metadata_chunks: set[str] = set()

        # Check metadata chunks
        for i, path in enumerate(lookup_paths):
            tmp_metadata_chunks, tmp_mp4_chunks = self._search_chunks_in_s3_path(
                path, bucket, messageid=messageid)

            tmp_metadata_chunks_filtered = set()
            tmp_metadata_striped = set()

            # Ensure that only metadata belonging to the video are checked and return
            for chunk in tmp_metadata_chunks:
                mp4_key = recording_regex.search(chunk).group(1)
                if mp4_key in mp4_chunks_left:
                    tmp_metadata_chunks_filtered.add(chunk)
                    tmp_metadata_striped.add(mp4_key)

            metadata_chunks = metadata_chunks.union(
                tmp_metadata_chunks_filtered)

            mp4_chunks_left = mp4_chunks_left - tmp_metadata_striped

            # If all metadata chunks were found returns
            if len(mp4_chunks_left) == 0:
                LOGGER.info(
                    "Metadata found outside of upload bounds, %d paths crawled", i)
                return True, metadata_chunks

        LOGGER.warning(
            "Fail to validate metadata for the following video chunks: %s", str(mp4_chunks_left))
        return False, metadata_chunks

    def check_metadata_exists_and_is_complete(self, message: VideoMessage) -> tuple[bool, set[str]]:
        """
        Checks if all metadata exists and are complete in RCC S3.

        Args:
            message (VideoMessage): The video message coming from the queue.

        Returns:
            Tuple[
                bool,       :True if metadata is complete, False otherwise
                set[str]    :A set with all metadata chunks.
            ]
        """

        bucket = self.CS.rcc_info["s3_bucket"]

        s3_object_params = RCCS3ObjectParams(
            s3_path=f'{message.tenant}/{message.deviceid}/', bucket=bucket)

        # Make sure it has access to tenant and device
        if not ContainerServices.check_if_tenant_and_deviceid_exists_and_log_on_error(
                self.RCC_S3_CLIENT, s3_object_params, message.messageid):
            return False, set()

        # Get all mp4 lookup paths
        mp4_lookup_paths = self._get_chunks_lookup_paths(message)

        if not mp4_lookup_paths:
            LOGGER.error("No video chunks paths found for %s", f'{message.tenant}/{message.deviceid}/', extra={
                "messageid": message.messageid})
            return False, set()

        LOGGER.debug("Searching for chunks")

        # Store all chunks in it's set
        mp4_chunks_left: set[str] = set()
        metadata_chunks: set[str] = set()

        recording_regex = re.compile(r"/hour=[0-9]{2}/(.+\.mp4).*")

        # Search for video and metadata chunks
        for path in mp4_lookup_paths:
            tmp_metadata_chunks, tmp_mp4_chunks = self._search_chunks_in_s3_path(
                path, bucket, messageid=message.messageid, start_time=message.uploadstarted, end_time=message.uploadfinished)

            # Store only the recording name to ignore the folders before
            tmp_mp4_chunks = [recording_regex.search(
                chunk).group(1) for chunk in tmp_mp4_chunks]

            metadata_chunks = metadata_chunks.union(tmp_metadata_chunks)
            mp4_chunks_left = mp4_chunks_left.union(tmp_mp4_chunks)

        tmp_metadata_filtered: set[str] = set()
        tmp_metadata_striped: set[str] = set()

        if len(mp4_chunks_left) == 0:
            LOGGER.error(
                "Could not find any video chunks for %s. Probably the chunks are out of upload bounds",
                f'{message.tenant}/{message.deviceid}/')
            return False, set()

        # Ensure that only metadata belonging to the video are checked
        for chunk in metadata_chunks:

            # Strips the video from the metadata name
            mp4_key = recording_regex.search(chunk).group(1)

            if mp4_key in mp4_chunks_left:
                tmp_metadata_filtered.add(chunk)
                tmp_metadata_striped.add(mp4_key)

        metadata_chunks = tmp_metadata_filtered

        # Remove video chunks where a metadata chunk was found
        mp4_chunks_left = mp4_chunks_left - tmp_metadata_striped

        if not mp4_chunks_left:
            LOGGER.info(
                "all metadata chunks found within upload bounds")
            return True, metadata_chunks

        LOGGER.debug("Not all metadata found within upload bounds, searching until %s", str(datetime.now()))

        # Search for the metadata paths until the current day
        metadata_paths = self._get_chunks_lookup_paths(message,
                                                       start_time=message.uploadfinished +
                                                       timedelta(hours=1),
                                                       end_time=datetime.now()
                                                       )

        # Search for missing metadata chunks until the current day
        all_found, tmp_metadata_chunks = self._search_for_match_chunks(
            metadata_paths, mp4_chunks_left, bucket, message.messageid)

        metadata_chunks = metadata_chunks.union(tmp_metadata_chunks)

        LOGGER.info("Found %d metadata chunks", len(metadata_chunks))

        return all_found, metadata_chunks

    def _get_metadata_chunks(self, video_msg: VideoMessage, metadata_chunk_paths: set[str]):
        """Download metadata chunks from RCC S3

        Args:
            video_msg (VideoMessage): Message object
            metadata_chunk_paths (set[str]): A set containing all the chunks

        Returns:
            chunks (dict): Dictionary with all raw metadata chunks between the bounds defined, indexed by their relative order. Defaults to {}}.
        """

        chunks = {}
        chunks_count = 0

        '''Cycle through the received list of matching files, download them from S3 and store them on the files_dict dictionary'''
        for file_name in metadata_chunk_paths:
            if file_name.endswith('.json.zip') or file_name.endswith('.json'):
                if file_name.endswith('.json.zip'):
                    # Download metadata file from RCC S3 bucket
                    compressed_metadata_file = self.CS.download_file(
                        self.RCC_S3_CLIENT, self.CS.rcc_info["s3_bucket"], file_name)
                    metadata_bytes = gzip.decompress(
                        compressed_metadata_file)

                # Process only json files
                elif file_name.endswith('.json'):
                    # Download metadata file from RCC S3 bucket
                    metadata_bytes = self.CS.download_file(
                        self.RCC_S3_CLIENT, self.CS.rcc_info["s3_bucket"], file_name)

                else:
                    raise RuntimeError(
                        f"Metadata file {file_name} has an unknown file format")

                # Read all bytes from http response body
                # (botocore.response.StreamingBody) and convert them into json format
                json_temp = json.loads(metadata_bytes.decode(
                    "utf-8"), object_pairs_hook=self._json_raise_on_duplicates)
                json_temp["filename"] = file_name
                chunks[chunks_count] = json_temp
                chunks_count += 1

        if not chunks:
            LOGGER.warning(
                f"Could not find any metadata files for {video_msg.recording_type} {video_msg.recordingid}",
                extra={
                    "messageid": video_msg.messageid})
        return chunks

    def _process_chunks_into_mdf(self, chunks, video_msg):
        """Extract metadata from raw chunks and transform it into MDF data

        Args:
            chunks (dict): Dictionary of chunks, indexed by their relative order
            video_msg (VideoMessage): Message object

        Returns:
            resolution (str): Video resolution
            pts (dict): Video timebounds, as partial timestamps and as UTC
            mdf_data(list): list with frame metadata, sorted by relative index
        """

        # this enables support for metadata version 0.4.2 from new IVS
        pts_key = 'chunk' if 'chunk' in chunks[0] else 'chunkPts'
        utc_key = 'chunk' if 'chunk' in chunks[0] else 'chunkUtc'

        # Calculate the bounds for partial timestamps - the start of the earliest and the end of the latest
        starting_chunk_time_pts = min(
            [int(chunks[id][pts_key]['pts_start']) for id in chunks.keys()])
        starting_chunk_time_utc = min(
            [int(chunks[id][utc_key]['utc_start']) for id in chunks.keys()])
        ending_chunk_time_pts = max(
            [int(chunks[id][pts_key]['pts_end']) for id in chunks.keys()])
        ending_chunk_time_utc = max(
            [int(chunks[id][utc_key]['utc_end']) for id in chunks.keys()])
        pts = {
            "pts_start": starting_chunk_time_pts,
            "pts_end": ending_chunk_time_pts,
            "utc_start": starting_chunk_time_utc,
            "utc_end": ending_chunk_time_utc
        }

        # Resolution is the same for the entire video
        resolution = chunks[0]['resolution']

        # Build sorted list with all frame metadata, sorted
        frames = []
        for chunk in chunks:
            if chunks[chunk].get('frame'):
                for frame in chunks[chunk]["frame"]:
                    frames.append(frame)
            else:
                LOGGER.warning(
                    f"No frames in metadata chunk -> {chunks[chunk]}", extra={"messageid": video_msg.messageid})

        # Sort frames by number
        mdf_data = sorted(frames, key=lambda x: int(itemgetter("number")(x)))

        return resolution, pts, mdf_data

    def _upload_source_data(self, source_data, video_msg, video_id: str):
        """Store source data on our raw_s3 bucket

        Args:
            source_data (dict): data to be stored
            video_msg (VideoMessage): Message object

        Returns:
            s3_upload_path (str): Path where file got stored. Returns None if upload fails.
        """

        if "srxdriverpr" in video_msg.streamname:
            s3_folder = self.CS.sdr_folder['driver_pr']
        else:
            s3_folder = self.CS.sdr_folder['debug']
        source_data_as_bytes = bytes(json.dumps(
            source_data, ensure_ascii=False, indent=4).encode('UTF-8'))
        bucket = self.CS.raw_s3
        s3_path = f"{s3_folder}{video_id}{METADATA_FILE_EXT}"
        try:
            self.CS.upload_file(
                self.S3_CLIENT, source_data_as_bytes, bucket, s3_path)
            LOGGER.info(
                f"Successfully uploaded to {bucket}/{s3_path}", extra={"messageid": video_msg.messageid})
        except Exception as exception:
            if ContainerServices.check_s3_file_exists(self.S3_CLIENT, bucket, s3_path):
                LOGGER.info(f"File {s3_path} already exists in {bucket} -> {repr(exception)}", extra={
                    "messageid": video_msg.messageid})
            else:
                LOGGER.error(f"File {s3_path} could not be uploaded onto {bucket} -> {repr(exception)}", extra={
                             "messageid": video_msg.messageid})
                return

        return s3_path

    def ingest(self, video_msg: VideoMessage, video_id: str, metadata_chunk_paths: set[str]):
        # Fetch metadata chunks from RCC S3
        chunks = self._get_metadata_chunks(video_msg, metadata_chunk_paths)
        if not chunks:
            LOGGER.warning("Cannot ingest metadata from empty set of chunks", extra={
                "messageid": video_msg.messageid})
            return False
        # Process the raw metadata into MDF (fields 'resolution', 'chunk', 'frame', 'chc_periods')
        resolution, pts, frames = self._process_chunks_into_mdf(
            chunks, video_msg)

        # Build source file to be stored - 'source_data' is the MDF, extended with
        # the original queue message and its identifier
        source_data = {
            "messageid": video_msg.messageid,
            "message": video_msg.raw_message,
            "resolution": resolution,
            "chunk": pts,
            "frame": frames
        }
        mdf_s3_path = self._upload_source_data(
            source_data, video_msg, video_id)

        # Notify MDFP with new metadata processing request
        # mdfp_queue = self.CS.sqs_queues_list["MDFParser"] # not on config, it was agreed we should walk away from it
        mdfp_queue = os.environ["QUEUE_MDFP"]

        if mdf_s3_path:
            message_for_mdfp = dict(
                _id=video_id, s3_path=f"s3://{self.CS.raw_s3}/{mdf_s3_path}")
            try:
                self.CS.send_message(
                    self.SQS_CLIENT, mdfp_queue, message_for_mdfp)
                return True
            except Exception as exception:
                LOGGER.error(f"Could not send message to queue {mdfp_queue} -> {repr(exception)}", extra={
                             "messageid": video_msg.messageid})
        else:
            LOGGER.info(f"Will not send message to queue {mdfp_queue} as source data could not be uploaded ->", extra={
                        "messageid": video_msg.messageid})
        return False
