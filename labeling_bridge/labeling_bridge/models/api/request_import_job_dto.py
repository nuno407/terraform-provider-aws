""" API Models """
from pydantic import BaseModel, Field


class RequestImportJobDTO(BaseModel):  # pylint: disable=too-few-public-methods
    """ RequestImportJobDTO """
    dataset_name: str = Field(alias="dataset")
    kognic_project_id: str = Field(alias="kognicProjectId")
    labelling_job_name: str = Field(alias="labellingJobName")
    client_id: str = Field(alias="clientId")
    client_secret: str = Field(alias="clientSecret")
