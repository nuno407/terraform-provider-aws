import fiftyone as fo
import fiftyone.core.storage as fos
 
client = fos.get_client("s3")
client.get_file_metadata("s3://dev-rcd-anonymized-video-files/Debug_Lync/AGO_voxel_test_anonymized.mp4")
