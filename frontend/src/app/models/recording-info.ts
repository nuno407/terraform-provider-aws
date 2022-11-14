import { Snapshot } from './snapshots';

export interface RootObject {
  message: Message[];
  statusCode: string;
}

export interface RecordingOverviewObject {
  message: Message[];
  pages: number;
  total: number;
  statusCode: string;
}

export interface SingleObject {
  message: Message;
  statusCode: string;
}

export interface Message {
  _id: string;
  processing_list: [];
  snapshots: string;
  number_chc_events: string;
  lengthCHC: string;
  lq_video: LqVideoInfo;
  data_status: string;
  description: string;
  snapshots_paths: string[];
  parsed_snapshots: Snapshot[];
  length: string;
  time: string;
  resolution: string;
  deviceID: string;
  last_updated: Date;
  tenant: string;
}

export interface LqVideoInfo {
  id: string;
  length: string;
  resolution: string;
  snapshots: string;
  time: string;
}

/**Interface for s3 bucket video */
export interface VideoInfo {
  message: string;
  statusCode: number;
}

/**Video*/
export interface Message {
  message: { [key: string]: string };
}

export interface RecordingInfo {
  recordingInfoId: string;
  startTimestamp: number;
  endTimestamp: number;
  incidentId?: string;
  uploadId?: string;
}
