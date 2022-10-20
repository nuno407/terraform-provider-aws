export interface Query {
  _id: string;
  data_status: string;
  from_container: string;
  info_source: string;
  last_updated: Date;
  processing_list: string[];
  s3_path: string;
}
