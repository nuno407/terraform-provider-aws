"""API Controller"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from artifact_api.api.events_controller import events_router
from artifact_api.api.media_controller import media_router
from artifact_api.api.metadata_controller import metadata_router
from artifact_api.api.pipeline_controller import pipeline_router
from artifact_api.api.upload_rule_controller import upload_rule_router
from artifact_api.bootstrap import bootstrap_di


@asynccontextmanager
async def lifespan(_: FastAPI):
    """
    Callback to initilize the dependency injection

    Args:
        app (FastAPI): _description_
    """
    bootstrap_di()
    yield

app = FastAPI(lifespan=lifespan)
app.include_router(events_router)
app.include_router(media_router)
app.include_router(metadata_router)
app.include_router(pipeline_router)
app.include_router(upload_rule_router)


@app.get("/alive")
async def alive():
    """Healthcheck endpoint"""
    return {}
