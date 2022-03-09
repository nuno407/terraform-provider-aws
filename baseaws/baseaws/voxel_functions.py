from datetime import timedelta as td, datetime

def create_dataset(bucket_name):
    import fiftyone as fo
    import eta.core.frameutils as etaf
    from fiftyone import ViewField as F

    if (fo.dataset_exists(bucket_name)):
        dataset = fo.load_dataset(bucket_name)
    else:
        dataset = fo.Dataset(bucket_name,True)


def add_sample(data_set,sample_info):
    import fiftyone as fo
    import eta.core.frameutils as etaf
    from fiftyone import ViewField as F

    dataset = fo.load_dataset(data_set)


    #IF the sample already exists, update it's information, otherwise create a new one

    try:
        sample = dataset.one(F("pipeline_id") == sample_info["_id"])
      
    except Exception:
        sample = fo.Sample(filepath=sample_info["s3_path"])    

 


    #  Full create FiftyOne Sample object
    # 
    #     filepath = sample_info["s3_path"]
    #     sample = fo.Sample(filepath=filepath)
    # 
    #     # Parse and populate labels and metadata on sample

    #Setup mandatory field
    sample["pipeline_id"] = sample_info["_id"]
    # if sample_info.get("metadata_available")

    #Validate and populate optional fields 
    if sample_info["metadata_available"]:
        sample["metadata_available"] = sample_info["metadata_available"]        

    if sample_info["pipeline_stage"]:
        sample["pipeline_stage"] = sample_info["pipeline_stage"]        

    if sample_info["recording_overview"]["cameraID"]:
        sample["cameraID"] = sample_info["recording_overview"]["cameraID"] 

    if sample_info["recording_overview"]["resolution"]:
        sample["recording_resolution"] = sample_info["recording_overview"]["resolution"] 

    if sample_info["recording_overview"]["time"]:
        recording_time = sample_info["recording_overview"]["time"]
        recording_datetime = datetime.strptime(recording_time, "%Y-%m-%d %H:%M:%S")
        sample["recording_time"] = recording_datetime

    if sample_info["data_status"]:
        sample["data_status"] = sample_info["data_status"]

    if sample_info["from_container"]:
        sample["from_container"] = sample_info["from_container"]

    if sample_info["info_source"]:
        sample["info_source"] = sample_info["info_source"]

    if sample_info["last_updated"]:
        sample["last_updated"] = sample_info["last_updated"]

    if sample_info["processing_list"]:
        sample["processing_list"] = sample_info["processing_list"]


# 
#     # Add results as Regression Labels
#     for algo_results in sample_info["results"]:
#         alg = algo_results["algorithm"]
#         source = algo_results["source"]
#         label_field = alg        
#         for ind, chb in enumerate(algo_results["CHBs"]):
#             label = fo.Regression(
#                 value=float(chb),
#                 source=source,
#                 alg=alg,
#             )
#             frame_number = ind+1
#             # frame_number = etaf.timestamp_to_frame_number(timestamp, duration, total_frame_count)
#             sample.frames[frame_number][label_field] = label
# 
# 
    
    # Add sample to dataset
    dataset.add_sample(sample)

def update_sample(data_set,sample_info):
    import logging
    import fiftyone as fo
    import eta.core.frameutils as etaf
    from fiftyone import ViewField as F

    dataset = fo.load_dataset(data_set)


    #IF the sample already exists, update it's information, otherwise create a new one

    try:
        sample = dataset.one(F("pipeline_id") == sample_info["_id"])
      
    except Exception:
        sample = fo.Sample(filepath=sample_info["s3_path"])    

 


    #  Full create FiftyOne Sample object
    # 
    #     filepath = sample_info["s3_path"]
    #     sample = fo.Sample(filepath=filepath)
    # 
    #     # Parse and populate labels and metadata on sample

    #Setup mandatory field
    sample["pipeline_id"] = sample_info["_id"]

    #Validate and populate optional fields 
    try:
        sample["metadata_available"] = sample_info["metadata_available"]
    except Exception:
        logging.info("No metadata_available")

    try:
        sample["pipeline_stage"] = sample_info["pipeline_stage"]
    except Exception:
        logging.info("No pipeline_stage")

    try:
        sample["recording_overview"] = sample_info["recording_overview"]
    except Exception:
        logging.info("No recording_overview")

    try:
        sample["cameraID"] = sample_info["recording_overview"]["cameraID"] 
    except Exception:
        logging.info("No cameraID")

    try:
        sample["resolution"] = sample_info["recording_overview"]["resolution"] 
    except Exception:
        logging.info("No resolution")

    try:
        recording_time = sample_info["recording_overview"]["time"]
        recording_datetime = datetime.strptime(recording_time, "%Y-%m-%d %H:%M:%S")
        sample["recording_time"] = recording_datetime
    except Exception:
        logging.info("No time")

    try:
        sample["data_status"] = sample_info["data_status"]
    except Exception:
        logging.info("No data_status")        

    try:
        sample["from_container"] = sample_info["from_container"]
    except Exception:
        logging.info("No from_container")        

    try:
        sample["info_source"] = sample_info["info_source"]
    except Exception:
        logging.info("No info_source")        

    try:
        sample["last_updated"] = sample_info["last_updated"]
    except Exception:
        logging.info("No last_updated")        
        
    try:
        sample["processing_list"] = sample_info["processing_list"]
    except Exception:
        logging.info("No processing_list")        

        # 
#     # Add results as Regression Labels
#     for algo_results in sample_info["results"]:
#         alg = algo_results["algorithm"]
#         source = algo_results["source"]
#         label_field = alg        
#         for ind, chb in enumerate(algo_results["CHBs"]):
#             label = fo.Regression(
#                 value=float(chb),
#                 source=source,
#                 alg=alg,
#             )
#             frame_number = ind+1
#             # frame_number = etaf.timestamp_to_frame_number(timestamp, duration, total_frame_count)
#             sample.frames[frame_number][label_field] = label
# 
# 
    
    # Add sample to dataset
    sample.save()