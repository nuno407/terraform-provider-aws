# -*- coding: utf-8 -*-

import fiftyone as fo
import fiftyone.zoo as foz


dataset_zoo = foz.load_zoo_dataset("quickstart-video")
#session = fo.launch_app(dataset_zoo)


dataset_lync_dir = "/mnt/s3/Debug_Lync/"
dataset_lync_type = fo.types.VideoDirectory  

# Import the dataset
dataset_lync = fo.Dataset.from_dir(
    dataset_dir=dataset_lync_dir,
    dataset_type=dataset_lync_type,
    name="Lync",
)


session = fo.launch_app(dataset_lync)



session.wait(-1)


