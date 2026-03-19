from fastapi import APIRouter

router = APIRouter()


@router.post("")
def resolve() -> dict:
    # Stub endpoint for hackathon scaffolding.
    return {"status": "ok", "message": "resolve not implemented yet"}

