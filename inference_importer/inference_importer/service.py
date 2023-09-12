""" Inference Importer Service Module """
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import tempfile
import os
import json
import fiftyone as fo
from fiftyone import ViewField as F
import boto3
from mypy_boto3_s3 import S3Client

from base.aws.container_services import ContainerServices
from base.voxel import functions as voxel_functions
from base.voxel.constants import OPENLABEL_LABEL_MAPPING_INFERENCE, INFERENCE_POSE
from inference_importer.sqs_message import SQSMessage

AWS_REGION = os.environ.get("AWS_REGION", "eu-central-1")

_logger = ContainerServices.configure_logging(__name__)

fields_to_delete = ["OpenLABEL_id", "interpolated", "is_hole",
                    "mode", "name", "stream", "order", "timestamp", "job_id"]


class InferenceImporter():
    """
    Inference Importer Class
    Responsible for downloading and importing the results from the
    Amazon Sagemaker Inference Job into Voxel 51 UI
    """

    s3_client: S3Client

    def process(self, s3_client: S3Client, parsed_message: SQSMessage):  # pylint: disable=too-many-locals
        """
        process function:
        downloads file results from Inference to a temporary directory
        and imports them into voxel


        Args:
            s3_client (boto3.client)
            parsed_message (SQSMessage): parsed SQS Message
        """
        paginator = s3_client.get_paginator("list_objects_v2")
        # gets the bucket name from a s3://<bucket-name> path
        bucket_name = parsed_message.transform_job_output_path.split("/")[2]
        prefix = parsed_message.transform_job_output_path.replace("s3://" + bucket_name + "/", "")
        output_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        _logger.debug("bucket_name: %s, prefix: %s", bucket_name, prefix)
        output_inferences = []
        # DOWNLOAD FILES
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            with ThreadPoolExecutor(initializer=self.initialize_worker) as executor:
                for page in output_iterator:
                    if "Contents" in page:
                        for obj in page["Contents"]:
                            output_inferences.append(obj["Key"])
                            _logger.debug(output_inferences)
                            executor.submit(self.get_inference_from_s3, obj["Key"],
                                            tmp_dir_name, bucket_name,
                                            parsed_message.transform_job_name)

            fn_map = self.create_fn_map(output_inferences, parsed_message.source_bucket_name, tmp_dir_name, prefix)

            # IMPORT INTO VOXEL
            try:
                openlabel_skeleton = voxel_functions.openlabel_skeleton()
                dataset = fo.load_dataset(parsed_message.dataset_name)
                if dataset.skeletons.get(INFERENCE_POSE) is None:
                    _logger.debug("Inference pose skeletons not found, adding them")
                    voxel_functions.set_dataset_skeleton_configuration(dataset)

                dataset.merge_dir(dataset_type=fo.types.OpenLABELImageDataset,
                                  merge_lists=True,
                                  overwrite=False,
                                  data_path=fn_map,
                                  labels_path=tmp_dir_name,
                                  dynamic=True,
                                  insert_new=False,
                                  tags="inference",
                                  skeleton=openlabel_skeleton,
                                  skeleton_key="point_class",
                                  label_field=OPENLABEL_LABEL_MAPPING_INFERENCE
                                  )

                # delete extra fields from openlabel
                for field in fields_to_delete:
                    dataset.delete_sample_field(f"{INFERENCE_POSE}.keypoints." + field)
                dataset.save()

                # Get ids of labels where we need to set the date
                label_ids = dataset.filter_labels(
                    INFERENCE_POSE,
                    (~F("date").exists()) & (F("inference_job") == parsed_message.transform_job_name)
                ).values(f"{INFERENCE_POSE}.keypoints.id", unwind=True)
                # Map the date to the id
                _logger.debug(label_ids)
                today = datetime.combine(datetime.today(), datetime.min.time())
                values = {_id: today for _id in label_ids}
                # Set the label_values
                dataset.set_label_values(f"{INFERENCE_POSE}.keypoints.date", values)

                # Add all new fields
                dataset.add_dynamic_sample_fields()
                dataset.save()

            except Exception:  # pylint: disable=broad-except
                _logger.exception("Error in Voxel Import")

    def get_inference_from_s3(self, filepath: str, tmp_dir: str,
                              bucket_name: str, job_name: str):
        """Downloads the s3 file to the temporary directory

        Args:
            filepath (str): s3 filepath
            tmp_dir (str): temporary directory
            bucket_name (str): amazon sagemaker bucket name
            job_name (str): inference job name
        """
        try:
            filename = filepath.split("/")[-1].replace(".out", ".json")
            tmp_filepath = os.path.join(tmp_dir, filename)
            self.s3_client.download_file(bucket_name, filepath, tmp_filepath)
            _logger.debug("filename: %s, tmp_filepath: %s, \
                           tmp_dir: %s, filepath: %s",
                          filename, tmp_filepath, tmp_dir, filepath)
            # get the openlabel from the .out file
            inference_openlabel = {}
            with open(tmp_filepath, "r", encoding="utf-8") as file:
                json_data = json.load(file)
                inference_openlabel = json_data["openlabel"]
                if inference_openlabel["frames"]["0"].get("objects") is not None:
                    _logger.debug("Inference job object")
                    for an_object in inference_openlabel["frames"]["0"]["objects"]:
                        inference_openlabel["frames"]["0"]["objects"][an_object]["inference_job"] = job_name  # pylint: disable=line-too-long
                        inference_openlabel["frames"]["0"]["frame_properties"]["streams"]["Voxel_export"]["uri"] = os.path.basename(filepath).replace(".out", "")  # pylint: disable=line-too-long # noqa
            with open(tmp_filepath, "w", encoding="utf-8") as json_file:
                new_json = {"openlabel": inference_openlabel}
                _logger.debug(new_json)
                json.dump(new_json, json_file, indent=6)

        except Exception:  # pylint: disable=broad-except
            _logger.exception("Error in thread")

        # verify if the openlabel format is valid

    def initialize_worker(self):
        """
        Initiliazes the worker thread with it's own s3 client
        This is needed since downloads from same boto3 client are not thread safe
        """
        self.s3_client = boto3.client("s3", region_name=AWS_REGION)

    def create_fn_map(self, output_inferences: list[str],
                      source_bucket_name: str, tmp_dir_name: str, prefix: str) -> dict:
        """
        Creates a dict to map the source s3 filepaths(used as an id in voxel)
        to their filenames locally

        Args:
            output_inferences (list[str]): output inferences paths inside amazon sagemaker bucket
            source_bucket_name (str): bucket name of the source image files
            tmp_dir_name (str): temporary directory name
            prefix (str): s3 prefix to switch

        Returns:
            (dict): map of filename : filepath
        """
        fn_map = {}

        for inference_path in output_inferences:
            new_prefix = f"s3://{source_bucket_name}"
            inference_path = inference_path.replace(prefix, new_prefix)
            fn_map[inference_path.split("/")[-1].replace(".out", "")] = inference_path.replace(".out", "")

        _logger.debug(fn_map)
        _logger.debug(os.listdir(tmp_dir_name))

        return fn_map
