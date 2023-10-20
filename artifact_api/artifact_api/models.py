"""Models"""
from pydantic import BaseModel


class ResponseMessage(BaseModel):
    """Response Model"""
    message: str
