from fastapi import FastAPI

from .routers.health import router as health_router
from .routers.voice import router as voice_router


def create_app() -> FastAPI:
    app = FastAPI(title="VyapaarAI Backend", version="0.1.0")

    # Routers
    app.include_router(health_router)
    app.include_router(voice_router)

    return app


app = create_app()
