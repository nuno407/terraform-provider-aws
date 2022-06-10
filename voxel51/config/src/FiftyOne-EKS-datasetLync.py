# -*- coding: utf-8 -*-
import os
import fiftyone as fo
import fiftyone.utils.video as fouv





#### DEBUG DATASETS
#create the dataset
#dataset = fo.Dataset()
#dataset = fo.load_dataset("RideCareDataSet")
#fo.list_datasets()

#Select videos to be loaded


#direct SR 
#Mp4
#dataset.add_sample(fo.Sample(filepath="s3://dev-rcd-anonymized-video-files/Debug_Lync/deepsensation_rc_srx_develop_ivs1hi_04_InteriorRecorder_1639315740000_1639315800000_anonymized.mp4"))
#dataset.add_sample(fo.Sample(filepath="s3://dev-rcd-anonymized-video-files/Debug_Lync/AGO_voxel_test_anonymized.mp4"))
#print("s3://"+os.environ['ANON_S3']+"/Debug_Lync/AGO_voxel_test_anonymized.mp4")
#dataset.add_sample(fo.Sample(filepath="s3://"+os.environ['ANON_S3']+"/Debug_Lync/AGO_voxel_test_anonymized.mp4"))

#session = fo.launch_app(dataset)

#### DEBUG DATASETS


session = fo.launch_app()



session.wait(-1)


