from fastapi import FastAPI
from src.api.routes.pipelines import router as pipelines_router
from src.utils.logging import configure_logging

configure_logging()

app = FastAPI(title="DataForge API", version="1.0.0")
app.include_router(pipelines_router, prefix="/api/v1/pipelines", tags=["pipelines"])


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
