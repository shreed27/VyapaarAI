from fastapi import APIRouter

router = APIRouter()


@router.get("")
def credit_score() -> dict:
    # Stub endpoint for hackathon scaffolding.
    return {"status": "ok", "message": "credit_score not implemented yet"}

