from datetime import timedelta as td, datetime
import logging

_logger = logging.getLogger(__name__)

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
    import fiftyone as fo
    import eta.core.frameutils as etaf
    from fiftyone import ViewField as F

    dataset = fo.load_dataset(data_set)

    #If the sample already exists, update it's information, otherwise create a new one
    if 'filepath' in sample_info:
        sample_info.remove('filepath')

    try:
        sample = dataset.one(F("video_id") == sample_info["video_id"])
      
    except Exception:
        sample = fo.Sample(filepath=sample_info["s3_path"])    
        dataset.add_sample(sample)

    for (i,j) in sample_info.items():
            if i.startswith('_'):
                i="ivs"+i
            if i.startswith('filepath'):
                i="ivs_"+i    
            sample[i] = j

    #  Full create FiftyOne Sample object
    # 
    #     filepath = sample_info["s3_path"]
    #     sample = fo.Sample(filepath=filepath)
    # 
    #     # Parse and populate labels and metadata on sample

    
    if 'recording_overview' in sample_info:
        for (i,j) in sample_info.get('recording_overview').items():
            if i.startswith('_'):
                i="ivs"+i
            sample[i] = j
        if 'time' in sample["recording_overview"]:
            time = sample["recording_overview"]["time"]
            #sample.update({'recording_time': datetime.strptime(time, "%Y-%m-%d %H:%M:%S")})
            sample["recording_time"] = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
            sample["Hour"] = sample["recording_time"].strftime("%H")
            sample["Day"] = sample["recording_time"].strftime("%d")
            sample["Month"] = sample["recording_time"].strftime("%b")
            sample["Year"] = sample["recording_time"].strftime("%Y")            
            _logger.info(sample["recording_time"])
        else:
            _logger.info("No time")
    else:
        _logger.info("No items in recording overview")
        _logger.info(sample_info.get('recording_overview'))

    # Add sample to dataset
    sample.save()
