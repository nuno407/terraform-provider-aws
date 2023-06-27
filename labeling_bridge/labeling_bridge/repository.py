""" Database """
from typing import Any, Optional
from fiftyone.core.view import DatasetView
from labeling_bridge.models.api import RequestExportJobDTO
from labeling_bridge.models.database import LabelingJob, LabelingJobTask, StatusDocument, Status, DataDeletion, \
    DataDeletionStatus, KognicLabelingType


class Repository:
    """Repository """

    @staticmethod
    def generate_job_and_task_entries(
            dataset_view: DatasetView,
            user_email: str,
            request_export_job_dto: RequestExportJobDTO,
    ) -> LabelingJob:
        """ Generates database entries for the current labelling job. """
        export_job_status = StatusDocument(status=Status.NEW)
        labeling_job = LabelingJob(created_by=user_email,
                                   voxel_dataset=request_export_job_dto.dataset_name,
                                   voxel_query_dump=dataset_view.to_dict(),
                                   kognic_project_id=request_export_job_dto.kognic_project_id,
                                   kognic_labeling_job_name=request_export_job_dto.labelling_job_name,
                                   import_export_status=export_job_status)
        labeling_job.save()

        file_paths, raw_file_paths = dataset_view.values(["filepath", "raw_filepath"])
        for labeling_type in request_export_job_dto.labelling_types:
            for filepath, raw_filepath in zip(file_paths, raw_file_paths):
                export_job_task_status = StatusDocument(status=Status.NEW)
                deletion_status = DataDeletion(status=DataDeletionStatus.NOT_REQUESTED)
                labeling_job_task = LabelingJobTask(
                    kognic_labeling_job=labeling_job,
                    media_filepath=filepath,
                    raw_media_filepath=raw_filepath,
                    data_deletion=deletion_status,
                    import_export_status=export_job_task_status,
                    kognic_labeling_type=KognicLabelingType[labeling_type.name])
                labeling_job_task.save()

        return labeling_job

    @staticmethod
    def get_labeling_jobs(kognic_project_id: str, kognic_labeling_job_name: str,
                          voxel_dataset_name: Optional[str] = None) -> list[LabelingJob]:
        """ Get labeling Jobs. """
        query = {
            "kognic_project_id": kognic_project_id,
            "kognic_labeling_job_name": kognic_labeling_job_name
        }
        if voxel_dataset_name:
            query["voxel_dataset"] = voxel_dataset_name
        return LabelingJob.objects(**query)  # pylint: disable=no-member

    @staticmethod
    def update_status_of_tasks(
        labeling_jobs: list[LabelingJob],
        kognic_labeling_types: list[KognicLabelingType],
        status: StatusDocument,
        extra_filters: Optional[dict] = None,
        extra_set: Optional[dict] = None
    ) -> None:
        """ Update status of current sample """
        query = {
            "kognic_labeling_job__in": labeling_jobs,
            "kognic_labeling_type__in": kognic_labeling_types
        }
        if extra_filters:
            query.update(extra_filters)

        set_expression: dict[str, Any] = {"set__import_export_status": status}
        if extra_set:
            set_expression.update({f"set__{key}": value for key, value in extra_set.items()})

        LabelingJobTask.objects(**query).update(**set_expression)  # pylint: disable=no-member

    @staticmethod
    def update_status_of_jobs(
            labeling_jobs: list[LabelingJob],
            status: StatusDocument
    ) -> None:
        """ Update status of multiple jobs """
        for labeling_job in labeling_jobs:
            labeling_job.import_export_status = status
            labeling_job.save()
