import uuid
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from src.pipeline.manager import run_pipeline

router = APIRouter()


class PipelineCreateRequest(BaseModel):
    name: str
    source_type: str
    sink_type: str


class TriggerResponse(BaseModel):
    run_id: str
    status: str


@router.post("/{pipeline_id}/trigger", response_model=TriggerResponse)
async def trigger_pipeline(pipeline_id: str) -> TriggerResponse:
    try:
        result = await run_pipeline(pipeline_id)
        if result == "already_running":
            return TriggerResponse(run_id="", status="already_running")
        return TriggerResponse(run_id=result, status="started")
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
