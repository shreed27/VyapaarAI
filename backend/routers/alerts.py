from fastapi import APIRouter

router = APIRouter()


@router.get("")
def alerts() -> dict:
    # Stub endpoint for hackathon scaffolding.
    return {"status": "ok", "message": "alerts not implemented yet"}

