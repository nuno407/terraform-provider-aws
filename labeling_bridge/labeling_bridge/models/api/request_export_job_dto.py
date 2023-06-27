""" API Models """
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class KognicLabelingTypeDTO(str, Enum):
    """ KognicLabelingTypeDTO """
    BODYPOSE = "Splines"
    SEMSEG = "2D_semseg"


class RequestExportMethodDTO(str, Enum):
    """ RequestExportMethodDTO """
    TAG = "tag"
    FILTER = "filter"


class RequestExportJobDTO(BaseModel):  # pylint: disable=too-few-public-methods
    """ RequestExportJobDTO """
    dataset_name: str = Field(alias="dataset")
    kognic_project_id: str = Field(alias="kognicProjectId")
    labelling_types: list[KognicLabelingTypeDTO] = Field(alias="labellingType")
    labelling_job_name: str = Field(alias="labellingJobName")
    labelling_guidelines: str = Field(alias="labellingGuidelines")
    voxel_export_method: RequestExportMethodDTO = Field(alias="voxelExportMethod")
    client_id: str = Field(alias="clientId")
    client_secret: str = Field(alias="clientSecret")
    tag: Optional[str] = Field(alias="voxelTagToExport")
    filters: Optional[dict] = Field(alias="filters")
    stages: Optional[list[dict]] = Field(alias="stages")
