from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import voice, credit_score, resolve, alerts
import uvicorn

app = FastAPI(title="Dukaan Dost API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(voice.router, prefix="/voice", tags=["voice"])
app.include_router(credit_score.router, prefix="/credit-score", tags=["credit"])
app.include_router(resolve.router, prefix="/resolve", tags=["resolve"])
app.include_router(alerts.router, prefix="/alerts", tags=["alerts"])


@app.get("/health")
def health():
    return {"status": "ok", "service": "Dukaan Dost API"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

