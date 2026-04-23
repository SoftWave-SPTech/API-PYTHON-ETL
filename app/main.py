from fastapi import FastAPI

from app.config import settings
from app.routers.etl import router as etl_router

app = FastAPI(title=settings.api_title, version=settings.api_version)
app.include_router(etl_router)


@app.get("/health")
def health():
    return {"status": "ok"}
