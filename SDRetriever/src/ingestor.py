
import gzip
import json
import logging as log
import os
import re
import subprocess
from abc import abstractmethod
from datetime import datetime
from datetime import timedelta
from datetime import timedelta as td
from operator import itemgetter
from typing import Iterator
from typing import List
from typing import TypeVar

import boto3
from botocore.exceptions import ClientError
from message import VideoMessage

METADATA_FILE_EXT = '_metadata_full.json' # file format for metadata stored on DevCloud raw S3
FRAME_BUFFER = 120*1000 # 2minin milliseconds
ST = TypeVar('ST', datetime, str, int)  # SnapshotTimestamp type
LOGGER = log.getLogger("SDRetriever")

class Ingestor(object):
    
    def __init__(self, container_services, s3_client, sqs_client, sts_helper) -> None:
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
    
    def check_if_exists(self, s3_path: str, bucket=None, s3_client=None, messageid=None) -> tuple[bool, dict]:
        """check_if_exists - Verify if path exists on target S3 bucket.

        Args:
            s3_path (str): S3 path to look for.
        """
        # Check if there is a file with the same name already
        # stored on target S3 bucket
        if s3_client is None:
            s3_client = self.RCC_S3_CLIENT
        try:
            response_list = s3_client.list_objects_v2(
                Bucket=self.CS.raw_s3 if bucket is None else bucket,
                Prefix=s3_path
            )
        except:
            fields = s3_path.split("/") # just for relative paths
            tenant = fields[0]
            deviceid = fields[1]
            try:
                # can we access the tenant? (not by default, accessible tenants must be whitelisted on RCC by RideCare Cloud Operations)
                prefix = f"{tenant}/"
                response_list = s3_client.list_objects_v2(Bucket=self.CS.raw_s3 if bucket is None else bucket, Prefix=prefix)
            except:
                LOGGER.error(f"Could not access {bucket}/{prefix} - our AWS IAM role is likely forbidden from accessing tenant {tenant}", extra={"messageid": messageid})
            else:
                prefix = f"{tenant}/{deviceid}/"
                try:
                    # does the device exist?
                    response_list = s3_client.list_objects_v2(Bucket=self.CS.raw_s3 if bucket is None else bucket, Prefix=prefix)
                except:
                    LOGGER.error(f"Could not access folder {bucket}/{prefix} - Tenant {tenant} is accessible, but could not access device {deviceid}", extra={"messageid": messageid})
                else:
                    LOGGER.error(f"Could not access folder {s3_path}, something went wrong", extra={"messageid": messageid})
            return False, dict()
        
        return False if response_list is None else response_list.get('KeyCount', 0) != 0, response_list

    @property
    def RCC_S3_CLIENT(self):
        client = boto3.client('s3',
                              region_name='eu-central-1',
                              aws_access_key_id=self.accesskeyid,
                              aws_secret_access_key=self.secretaccesskey,
                              aws_session_token=self.sessiontoken)
        return client

    @property
    def accesskeyid(self):
        self._rcc_credentials = self.STS_HELPER.get_credentials()
        return self._rcc_credentials['AccessKeyId']

    @property
    def secretaccesskey(self):
        self._rcc_credentials = self.STS_HELPER.get_credentials()
        return self._rcc_credentials['SecretAccessKey']

    @property
    def sessiontoken(self):
        self._rcc_credentials = self.STS_HELPER.get_credentials()
        return self._rcc_credentials['SessionToken']


class VideoIngestor(Ingestor):
    def __init__(self, container_services, s3_client, sqs_client, sts_helper) -> None:
        super().__init__(container_services, s3_client, sqs_client, sts_helper)
        self.clip_ext = ".mp4"
        self.STREAM_TIMESTAMP_TYPE = 'PRODUCER_TIMESTAMP'

    @staticmethod
    def _ffmpeg_probe_video(video_bytes) -> dict:
        # Store bytes into current working directory as video
        temp_video_file = "input_video.mp4"
        with open(temp_video_file, "wb") as f:
            f.write(video_bytes)

        # Execute ffprobe command to get video clip info
        result = subprocess.run(["ffprobe", "-v", "error", "-show_format", "-show_streams",
                                "-print_format", "json", temp_video_file], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # Remove temporary file
        subprocess.run(["rm", temp_video_file])

        # Load ffprobe output (bytes) as JSON
        decoded_info = (result.stdout).decode("utf-8")
        video_info = json.loads(decoded_info)
        return video_info

    def ingest(self, video_msg):

        '''Obtain video from KinesisVideoStreams and upload it to our raw data S3'''
        # Define the target path where to place the video
        if "srxdriverpr" in video_msg.streamname:
            s3_folder = self.CS.sdr_folder['driver_pr']
        else:
            s3_folder = self.CS.sdr_folder['debug']

        s3_filename = f"{video_msg.streamname}_{video_msg.footagefrom}_{video_msg.footageto}"    
        s3_path = s3_folder + s3_filename + self.clip_ext

        # Get clip from KinesisVideoStream
        try:
            # Requests credentials to assume specific cross-account role on Kinesis
            role_credentials = self.STS_HELPER.get_credentials()
            video_from = datetime.fromtimestamp(video_msg.footagefrom/1000.0).strftime('%Y-%m-%d %H:%M:%S')
            video_to = datetime.fromtimestamp(video_msg.footageto/1000.0).strftime('%Y-%m-%d %H:%M:%S')
            video_bytes = self.CS.get_kinesis_clip(role_credentials, video_msg.streamname, video_from, video_to, self.STREAM_TIMESTAMP_TYPE)
        except Exception as exception:
            LOGGER.error(f"Could not obtain Kinesis clip from stream {video_msg.streamname} between {repr(video_from)} and {repr(video_to)} -> {repr(exception)}", extra={"messageid": video_msg.messageid})
            return None, None

        # Upload video clip into raw data S3 bucket
        try:
            self.CS.upload_file(self.S3_CLIENT, video_bytes, self.CS.raw_s3, s3_path)
            LOGGER.info(f"Successfully uploaded to {self.CS.raw_s3}/{s3_path}", extra={"messageid": video_msg.messageid})
        except Exception as exception:
            LOGGER.error(f"Could not upload file to {s3_path} -> {repr(exception)}", extra={"messageid": video_msg.messageid})
            return None, None

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
            "media_type":"video",
            "s3_path": self.CS.raw_s3 + "/" + s3_path,
            "footagefrom":video_msg.footagefrom,
            "footageto":video_msg.footageto,
            "tenant":video_msg.tenant,
            "deviceid":video_msg.deviceid,
            'length': str(td(seconds=video_seconds)),
            '#snapshots': str(0),
            'snapshots_paths': [],
            "sync_file_ext": "",
            'resolution': width + "x" + height,
            }

        # Generate dictionary with info to send to Selector container for training data request (high quality )
        is_interior_recording = video_msg.video_recording_type() == 'InteriorRecorder'
        if is_interior_recording:
            hq_request = {
                "streamName": f"{video_msg.tenant}_{video_msg.deviceid}_InteriorRecorder",
                "deviceId": video_msg.deviceid,
                "footageFrom": video_msg.footagefrom - FRAME_BUFFER, #Selector needs 
                "footageTo": video_msg.footageto + FRAME_BUFFER
            }
            # Send message to secondary input queue of Selector container
            hq_selector_queue = self.CS.sqs_queues_list["HQ_Selector"]
            self.CS.send_message(self.SQS_CLIENT, hq_selector_queue, hq_request)

        return db_record_data


class SnapshotIngestor(Ingestor):
    def __init__(self, container_services, s3_client, sqs_client, sts_helper) -> None:
        super().__init__(container_services, s3_client, sqs_client, sts_helper)
    
    @staticmethod
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
        start_timestamp = datetime.fromtimestamp(start/1000.0) if type(start) != datetime else start
        end_timestamp = datetime.fromtimestamp(end/1000.0) if type(end) != datetime else end
        if int(start.timestamp()) < 0 or int(end.timestamp()) < 0:
            return []

        dt = start_timestamp
        times = []
        searches = dict()
        # for all hourly intervals between start_timestamp and end_timestamp
        while dt <= end_timestamp or (dt.hour == end_timestamp.hour):
            # append its respective path to the list
            year = dt.strftime("%Y"); month = dt.strftime("%m")
            day = dt.strftime("%d"); hour = dt.strftime("%H")
            times.append(f"{tenant}/{device}/year={year}/month={month}/day={day}/hour={hour}/")
            dt = dt + td(hours=1)
            searches.update({tenant:{device:{year:{month:{day:{hour}}}}}})
        # and return it
        return times, searches

    def ingest(self, snap_msg):
        flag_do_not_delete = False
        uploads = 0
        # For all snapshots mentioned within, identify its snapshot info and save it - (current file name, timestamp to append)
        for chunk in snap_msg.chunks:

            # Our SRX device is in Portugal, 1h diff to AWS
            timestamp_10 = datetime.fromtimestamp(chunk.start_timestamp_ms/1000.0) #+ td(hours=-1.0)
            if chunk.uuid.endswith(".jpeg"):

                RCC_S3_bucket = self.CS.rcc_info.get('s3_bucket')
                # Define its new name by adding the timestamp as a suffix
                snap_name = f"{snap_msg.tenant}_{snap_msg.deviceid}_{chunk.uuid[:-5]}_{chunk.start_timestamp_ms}.jpeg"
                exists_on_devcloud, _ = self.check_if_exists('Debug_Lync/'+snap_name, self.CS.raw_s3, self.S3_CLIENT, snap_msg.messageid)
                if not exists_on_devcloud:

                    # Determine where to search for the file on RCC S3
                    possible_locations, searches = self._snapshot_path_generator(snap_msg.tenant, snap_msg.deviceid, timestamp_10, datetime.now())
                    LOGGER.debug(f"Generated search paths: {searches}", extra={"messageid": snap_msg.messageid})

                    # For all those locations
                    for folder in possible_locations:
                        
                        # If the file exists in this RCC folder
                        found_on_rcc, _ = self.check_if_exists(folder+chunk.uuid, RCC_S3_bucket, self.RCC_S3_CLIENT, snap_msg.messageid)
                        if found_on_rcc:
                            # Download jpeg from RCC 
                            try:
                                snapshot_bytes = self.CS.download_file(self.RCC_S3_CLIENT, RCC_S3_bucket, folder+chunk.uuid)
                            except ClientError as e:
                                flag_do_not_delete = True
                                if e.response['Error']['Code'] == 'NoSuchKey':
                                    LOGGER.exception(f"Download failed because file was found but could not be downloaded on {RCC_S3_bucket}/{folder+chunk.uuid} - {e}. Will try again later.", extra={"messageid": snap_msg.messageid})
                                break
                            # Upload to DevCloud
                            self.CS.upload_file(self.S3_CLIENT, snapshot_bytes, self.CS.raw_s3, 'Debug_Lync/'+snap_name)
                            LOGGER.info(f"Successfully uploaded to {self.CS.raw_s3}/{'Debug_Lync/'+snap_name}", extra={"messageid": snap_msg.messageid})
                            
                            db_record_data = {
                                "_id": snap_name[:-5],
                                "s3_path": f"{self.CS.raw_s3}/Debug_Lync/{snap_name}",
                                "deviceid": snap_msg.deviceid,
                                "timestamp": chunk.start_timestamp_ms,
                                "tenant": snap_msg.tenant,
                                "media_type": "image"
                            }
                            self.CS.send_message(self.SQS_CLIENT, self.metadata_queue, db_record_data)
                            LOGGER.info(f"Message sent to {self.metadata_queue} to create record for snapshot ", extra={"messageid": snap_msg.messageid})  

                            uploads +=1
                            # Stop the search
                            break
                    
                    if not found_on_rcc and not chunk.available:
                        LOGGER.info(f"File {chunk.uuid} is not yet available in RCC S3, with upload status {chunk.upload_status}", extra={"messageid": snap_msg.messageid})
                        LOGGER.info(f"Message will be re-ingested later", extra={"messageid": snap_msg.messageid})
                        flag_do_not_delete = True
                    elif not found_on_rcc and chunk.available:
                        LOGGER.info(f"Could not find {chunk.uuid} in RCC S3, with upload status {chunk.upload_status}", extra={"messageid": snap_msg.messageid})
                else:
                    LOGGER.info(f"File {'Debug_Lync/'+snap_name} already exists on {self.CS.raw_s3}" , extra={"messageid": snap_msg.messageid})
            else:
                LOGGER.info(f"Found something other than a snapshot: {chunk.uuid}", extra={"messageid": snap_msg.messageid})
        LOGGER.info(f"Uploaded {uploads}/{len(snap_msg.chunks)} snapshots into {self.CS.raw_s3}", extra={"messageid": snap_msg.messageid})
        return flag_do_not_delete

class MetadataIngestor(Ingestor):
    def __init__(self, container_services, s3_client, sqs_client, sts_helper) -> None:
        super().__init__(container_services, s3_client, sqs_client, sts_helper)

    @staticmethod
    def _json_raise_on_duplicates(ordered_pairs):
        """Convert duplicate keys to JSON array or if JSON objects, merges them."""
        d = {}
        for (k, v) in ordered_pairs:
            if k in d:
                if type(d[k]) is dict and type(v) is dict:
                    for(sub_k, sub_v) in v.items():
                        d[k][sub_k] = sub_v
                elif type(d[k]) is list:
                    d[k].append(v)
                else:
                    d[k] = [d[k], v]
            else:
                d[k] = v
        return d

    def _get_chunks_lookup_paths(self, message: VideoMessage) -> Iterator[str]:
        # Find the time bounds for the metadata
        metadata_start_time = message.uploadstarted.replace(microsecond=0, second=0, minute=0)
        metadata_end_time = message.uploadfinished.replace(microsecond=0, second=0, minute=0)

        # Calculate hours delta between start and end timestamps
        delta = metadata_end_time - metadata_start_time
        start_minutes = metadata_start_time.minute
        total_hours = int(delta.total_seconds() / 3600 + start_minutes / 60) + delta.days * 24 + 1

        for hour in range(total_hours):
            # Calculate the bounds for the hour
            upload_ts = metadata_start_time + timedelta(hours=hour)

            # Create the lookup path
            lookup_path = f"{message.tenant}/{message.deviceid}/year={upload_ts.year}/month={upload_ts.month:02}/day={upload_ts.day:02}/hour={upload_ts.hour:02}/{message.recording_type}_{message.recordingid}"

            yield lookup_path

    def check_metadata_exists_and_is_complete(self, message: VideoMessage):
        """Check if metadata exists and is complete.
        """

        # Get all lookup paths
        lookup_paths = list(self._get_chunks_lookup_paths(message))
        bucket = self.CS.rcc_info["s3_bucket"]
        if len(lookup_paths) == 0:
            LOGGER.info(f"No metadata chunk paths found for {message.recordingid}", extra={"messageid": message.messageid})
            return False

        for path in lookup_paths:
            metadata_exists, response = self.check_if_exists(
                path, bucket, messageid=message.messageid)
            if not metadata_exists:
                LOGGER.info(f"Metadata does not exist for {message.recordingid} at {path}", extra={
                            "messageid": message.messageid})
                return False
            filenames = [file_entry['Key']
                         for _, file_entry in response['Contents']]
            for videofile in [filename for filename in filenames if filename.endswith('.mp4')]:
                rexp = re.compile(videofile + r"\..+\.json(\.zip)?")
                if len(list(filter(rexp.match, filenames))) == 0:
                    LOGGER.debug(
                        f"Metadata not completely available in filenames list: {filenames}")
                    return False
        return True


    def _get_metadata_chunks(self, video_msg):
        """Download metadata chunks from RCC S3

        Args:
            metadata_start_time (epoch): Timestamp for lower bound
            metadata_end_time (epoch): Timestamp for upper bound
            video_msg (VideoMessage): Message object

        Returns:
            chunks (dict): Dictionary with all raw metadata chunks between the bounds defined, indexed by their relative order. Defaults to {}}.
        """

        chunks = {}
        chunks_count = 0

        # Generate a timestamp path for each hour within the calculated delta and get all files that match the key prefix
        for metadata_prefix in self._get_chunks_lookup_paths(video_msg):
            
            '''Check if there are any files on S3 with the prefix i.e. if metadata chunks _may_ exist'''
            bucket = self.CS.rcc_info["s3_bucket"]
            metadata_exists, response = self.check_if_exists(metadata_prefix, bucket, messageid = video_msg.messageid)

            '''Cycle through the received list of matching files, download them from S3 and store them on the files_dict dictionary'''
            for _, file_entry in enumerate(response['Contents']):
                file_name = file_entry['Key']
                if file_name.endswith('.json.zip') or file_name.endswith('.json'):
                    if file_name.endswith('.json.zip'):
                        # Download metadata file from RCC S3 bucket
                        compressed_metadata_file = self.CS.download_file(
                            self.RCC_S3_CLIENT, self.CS.rcc_info["s3_bucket"], file_name)
                        metadata_bytes = gzip.decompress(compressed_metadata_file)

                    # Process only json files
                    elif file_name.endswith('.json'):
                        # Download metadata file from RCC S3 bucket
                        metadata_bytes = self.CS.download_file(
                            self.RCC_S3_CLIENT, self.CS.rcc_info["s3_bucket"], file_name)

                    # Read all bytes from http response body
                    # (botocore.response.StreamingBody) and convert them into json format
                    json_temp = json.loads(metadata_bytes.decode("utf-8"), object_pairs_hook=self._json_raise_on_duplicates)
                    json_temp["filename"] = file_name
                    chunks[chunks_count] = json_temp
                    chunks_count += 1

        if not chunks:
            LOGGER.error(f"Could not find any metadata files for {video_msg.recording_type} {video_msg.recordingid}", extra={"messageid": video_msg.messageid})
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
        starting_chunk_time_pts = min([int(chunks[id][pts_key]['pts_start']) for id in chunks.keys()])
        starting_chunk_time_utc = min([int(chunks[id][utc_key]['utc_start']) for id in chunks.keys()])
        ending_chunk_time_pts = max([int(chunks[id][pts_key]['pts_end']) for id in chunks.keys()])
        ending_chunk_time_utc = max([int(chunks[id][utc_key]['utc_end']) for id in chunks.keys()])
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
                LOGGER.error(f"No frames in metadata chunk -> {chunks[chunk]}", extra={"messageid": video_msg.messageid})

        # Sort frames by number
        mdf_data = sorted(frames, key=lambda x: int(itemgetter("number")(x)))

        return resolution, pts, mdf_data

    def _upload_source_data(self, source_data, video_msg):
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
        source_data_as_bytes = bytes(json.dumps(source_data, ensure_ascii=False, indent=4).encode('UTF-8'))
        _id = f"{video_msg.streamname}_{video_msg.footagefrom}_{video_msg.footageto}"
        bucket = self.CS.raw_s3
        s3_path = f"{s3_folder}{_id}{METADATA_FILE_EXT}"
        try:
            self.CS.upload_file(self.S3_CLIENT, source_data_as_bytes, bucket, s3_path)
            LOGGER.info(f"Successfully uploaded to {bucket}/{s3_path}", extra={"messageid": video_msg.messageid})
        except Exception as exception:
            if self.check_if_exists(s3_path, bucket, messageid = video_msg.messageid):
                LOGGER.error(f"File {s3_path} already exists in {bucket} -> {repr(exception)}", extra={"messageid": video_msg.messageid})
            else:
                LOGGER.error(f"File {s3_path} could not be uploaded onto {bucket} -> {repr(exception)}", extra={"messageid": video_msg.messageid})
                return

        return _id, s3_path
        
    def ingest(self, video_msg):
        # Fetch metadata chunks from RCC S3
        chunks = self._get_metadata_chunks(video_msg)
        if not chunks:
            LOGGER.info("Cannot ingest metadata from empty set of chunks", extra={"messageid": video_msg.messageid})
            return False
        # Process the raw metadata into MDF (fields 'resolution', 'chunk', 'frame', 'chc_periods')
        resolution, pts, frames = self._process_chunks_into_mdf(chunks, video_msg)

        # Build source file to be stored - 'source_data' is the MDF, extended with the original queue message and its identifier
        source_data = {
            "messageid": video_msg.messageid,
            "message": video_msg.raw_message,
            "resolution":resolution,
            "chunk":pts,
            "frame":frames
            }
        mdf_id, mdf_s3_path = self._upload_source_data(source_data, video_msg)
        
        # Notify MDFP with new metadata processing request
        #mdfp_queue = self.CS.sqs_queues_list["MDFParser"] # not on config, it was agreed we should walk away from it
        mdfp_queue = os.environ["QUEUE_MDFP"]
        
        if mdf_s3_path:
            message_for_mdfp = dict(_id=mdf_id, s3_path=f"s3://{self.CS.raw_s3}/{mdf_s3_path}")
            try:
                self.CS.send_message(self.SQS_CLIENT, mdfp_queue, message_for_mdfp)
                return True
            except Exception as exception:
                LOGGER.error(f"Could not send message to queue {mdfp_queue} -> {repr(exception)}", extra={"messageid": video_msg.messageid})
        else:
            LOGGER.info(f"Will not send message to queue {mdfp_queue} as source data could not be uploaded ->", extra={"messageid": video_msg.messageid})
        return False
