""" Common code for multiple parsers. """
import os


def calculate_raw_s3_path(s3_bucket: str, tenant_id: str, artifact_id: str, file_extension: str) -> str:
    """ Calculate raw s3 path. """
    return os.path.join(f"s3://{s3_bucket}", tenant_id, f"{artifact_id}.{file_extension}")


def calculate_anonymized_s3_path(s3_bucket: str, tenant_id: str, artifact_id: str, file_extension: str) -> str:
    """ Calculate anonnymized s3 path. """
    return os.path.join(f"s3://{s3_bucket}", tenant_id, f"{artifact_id}_anonymized.{file_extension}")
