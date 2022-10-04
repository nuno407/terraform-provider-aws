from datetime import datetime
import logging
import fiftyone as fo
from fiftyone import ViewField as F

_logger = logging.getLogger(__name__)

def create_dataset(bucket_name):

    if (fo.dataset_exists(bucket_name)):
        dataset = fo.load_dataset(bucket_name)
    else:
        dataset = fo.Dataset(bucket_name,True)
        dataset.persistent = True


def update_sample(data_set,sample_info):

    dataset = fo.load_dataset(data_set)
    
    

    #If the sample already exists, update it's information, otherwise create a new one
    if 'filepath' in sample_info:
        sample_info.pop('filepath')

    try:
        sample = dataset.one(F("video_id") == sample_info["video_id"])
      
    except Exception:
        sample = fo.Sample(filepath=sample_info["s3_path"])    
        dataset.add_sample(sample)

    _logger.info("sample_info: %s !", sample_info)

    for (i,j) in sample_info.items():
            #print (i)
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
        for (k,l) in sample_info.get('recording_overview').items():
            if k.startswith('_'):
                k="ivs"+k
            try:
                print (k)
                print (l)
                sample[str(k)] = l
            except Exception as e:
                _logger.exception(f"sample[{k}] = {l}, {type(l)}")
                _logger.exception(f"{e}")
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

    #_logger.info("sample: %s !", sample)

    # Add sample to dataset
    sample.save()
