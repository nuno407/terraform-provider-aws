import re
import boto3

class S3Interaction:
    def __init__(self) -> None:
        self._s3_client = boto3.client('s3', region_name='eu-central-1')

    def _get_s3_path(self, raw_path)->tuple[str, str]:
        match = re.match(r'^s3://([^/]+)/(.*)$', raw_path)

        if(match is None or len(match.groups()) != 2):
            raise ValueError('Invalid MDF path: ' + raw_path)

        bucket = match.group(1)
        key = match.group(2)
        return bucket, key
