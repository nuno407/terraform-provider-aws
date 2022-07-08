from datetime import timedelta as td, datetime

def create_dataset(bucket_name):
    import fiftyone as fo
    import eta.core.frameutils as etaf
    from fiftyone import ViewField as F

    if (fo.dataset_exists(bucket_name)):
        dataset = fo.load_dataset(bucket_name)
    else:
        dataset = fo.Dataset(bucket_name,True)
        dataset.persistent = True


def update_sample(data_set,sample_info):
    import logging
    import fiftyone as fo
    import eta.core.frameutils as etaf
    from fiftyone import ViewField as F

    dataset = fo.load_dataset(data_set)


    #If the sample already exists, update it's information, otherwise create a new one

    try:
        sample = dataset.one(F("video_id") == sample_info["video_id"])
      
    except Exception:
        sample = fo.Sample(filepath=sample_info["s3_path"])    
        dataset.add_sample(sample)

    for (i,j) in sample_info.items():
            if i.startswith('_'):
                i="ivs"+i
            sample[i] = j

    #  Full create FiftyOne Sample object
    # 
    #     filepath = sample_info["s3_path"]
    #     sample = fo.Sample(filepath=filepath)
    # 
    #     # Parse and populate labels and metadata on sample

    

    if (sample_info.get('recording_overview',1)==1):
        for (i,j) in sample_info.get('recording_overview').items():
            if i.startswith('_'):
                i="ivs"+i
            sample[i] = j
        try:
            recording_time = sample_info["recording_overview"]["time"]
            recording_datetime = datetime.strptime(recording_time, "%Y-%m-%d %H:%M:%S")
            sample["recording_time"] = recording_datetime
        except Exception:
            logging.info("No time")
    else:
        logging.info("No recording_overview")

   
    # Add sample to dataset
    sample.save()
