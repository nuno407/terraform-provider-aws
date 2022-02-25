from operator import itemgetter

def generate_compact_mdf_metadata(files_dict):
    """TODO

    Arguments:
        TODO
    """
    aux_list = []

    key_set = set(("CameraViewBlocked", "cvb", "cve"))
    
    final_info = {
                    "frames": []
                }

    for frame in files_dict["frame"]:

        frame_data = {
                    "number": int(frame["number"]),
                    "timestamp": int(frame["number"])/15.7
        }

        if 'objectlist' in frame.keys():
            for item in frame['objectlist']:
                if item['id'] == '1':
                    frame_data["CameraViewBlocked"] = item['floatAttributes'][0]['value']
                if item['id'] == '4':
                    frame_data["cvb"] = item['floatAttributes'][0]['value']
                if item['id'] == '5':
                    frame_data["cve"] = item['floatAttributes'][0]['value']
                    
        if key_set.issubset(frame_data.keys()):
            aux_list.append(frame_data)

    final_info["frames"] = sorted(aux_list, key=lambda x: int(itemgetter("number")(x)))

    return final_info

def calculate_chc_periods(compact_mdf):
    frames_with_cv = []
    frame_times = {}

    #################################### Identify frames with cvb and cve equal to 1 #################################################################

    for frame in compact_mdf['frames']:
        if 'cvb' in frame and 'cve' in frame and 'timestamp' in frame and (frame["cvb"] == "1" or frame["cve"] == "1"):
            frames_with_cv.append(frame["number"])
            frame_times[frame["number"]] = frame["timestamp"]

    #################################### Group frames into events with tolerance #####################################################################

    frame_groups = group_frames_to_events(frames_with_cv, 2)

    #########  Duration calculation  #################################################################################################################
    chc_periods = []
    for frame_group in frame_groups:
        entry = {}
        entry['frames'] = frame_group
        entry['duration'] = (frame_times[frame_group[-1]] -
                             frame_times[frame_group[0]])
        chc_periods.append(entry)

    return chc_periods

def group_frames_to_events(frames, tolerance):
    groups = []

    if len(frames) < 1:
        return groups

    entry = []
    for i in range(0, len(frames)):
        entry.append(frames[i])
        if i == (len(frames) - 1) or abs(frames[i + 1] - frames[i]) > tolerance:
            groups.append(entry)
            entry = []

    return groups
