""" snapshot module """
import hashlib
import logging as log
from datetime import datetime, timedelta
from pathlib import Path
from typing import TypeVar

from base.aws.container_services import ContainerServices
from sdretriever.ingestor.ingestor import Ingestor


LOGGER = log.getLogger("SDRetriever." + __name__)

ST = TypeVar('ST', datetime, str, int)  # SnapshotTimestamp type


class SnapshotIngestor(Ingestor):
    """ Snapshot ingestor """

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

            if not chunk.uuid.endswith(".jpeg"):
                LOGGER.info(f"Found something other than a snapshot: {chunk.uuid}", extra={
                            "messageid": snap_msg.messageid})
                continue

            rcc_s3_bucket = self.container_svcs.rcc_info.get('s3_bucket')
            # Define its new name by adding the timestamp as a suffix
            uuid_no_format = Path(chunk.uuid).stem

            # Initialize file names
            snap_name = f"{snap_msg.tenant}_{snap_msg.deviceid}_{uuid_no_format}_{int(chunk.start_timestamp_ms)}.jpeg"
            metadata_name = f"{snap_msg.tenant}_{snap_msg.deviceid}_{uuid_no_format}_{int(chunk.start_timestamp_ms)}_metadata.json"

            # Checks if exists in devcloud
            exists_on_devcloud = ContainerServices.check_s3_file_exists(
                self.s3_client, self.container_svcs.raw_s3, snap_msg.tenant + "/" + snap_name)

            if exists_on_devcloud:
                LOGGER.info(
                    f"File {snap_msg.tenant}/{snap_name} already exists on {self.container_svcs.raw_s3}",
                    extra={
                        "messageid": snap_msg.messageid})
                continue

            # Generates the hash needed for healthcheck
            seed = Path(snap_name).stem
            internal_message_reference_id = hashlib.sha256(
                seed.encode("utf-8")).hexdigest()
            LOGGER.info("internal_message_reference_id generated hash=%s seed=%s",
                        internal_message_reference_id, seed)

            # Try to download the files
            try:
                jpeg_data = self.get_file_in_rcc(rcc_s3_bucket, snap_msg.tenant, snap_msg.deviceid,
                                                 chunk.uuid, timestamp_10, datetime.now(), [".jpeg", ".png"])

                metadata_data = self.get_file_in_rcc(rcc_s3_bucket, snap_msg.tenant, snap_msg.deviceid,
                                                     chunk.uuid, timestamp_10, datetime.now(), [".json"])

            except FileNotFoundError:
                LOGGER.warning("Either snapshot or metadata was not found retriyng later")
                flag_do_not_delete = True
                continue

            # Upload files to DevCloud
            self.container_svcs.upload_file(
                self.s3_client, jpeg_data, self.container_svcs.raw_s3, snap_msg.tenant + "/" + snap_name)

            self.container_svcs.upload_file(
                self.s3_client, metadata_data, self.container_svcs.raw_s3, snap_msg.tenant + "/" + metadata_name)

            LOGGER.info(f"Successfully uploaded to {self.container_svcs.raw_s3}/{snap_msg.tenant}/{snap_name}", extra={
                        "messageid": snap_msg.messageid})

            db_record_data = {
                "_id": snap_name[:-5],
                "s3_path": f"{self.container_svcs.raw_s3}/{snap_msg.tenant}/{snap_name}",
                "deviceid": snap_msg.deviceid,
                "timestamp": chunk.start_timestamp_ms,
                "tenant": snap_msg.tenant,
                "media_type": "image",
                "internal_message_reference_id": internal_message_reference_id,
            }
            self.container_svcs.send_message(
                self.sqs_client, self.metadata_queue, db_record_data)
            LOGGER.info(f"Message sent to {self.metadata_queue} to create record for snapshot ", extra={
                        "messageid": snap_msg.messageid})

            uploads += 1

        LOGGER.info(f"Uploaded {uploads}/{len(snap_msg.chunks)} snapshots into {self.container_svcs.raw_s3}",
                    extra={"messageid": snap_msg.messageid})
        return flag_do_not_delete
