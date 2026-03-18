from __future__ import annotations


def mock_stt(audio_b64: str | None) -> str:
    """
    Mock speech-to-text.

    For now we accept a base64 placeholder string and return a Hindi transcript.
    If audio is missing, return a generic Hindi prompt.
    """
    if not audio_b64:
        return "नमस्ते, आज का हिसाब बताइए।"
    # Deterministic placeholder output for demos.
    return "नमस्ते, मेरे आज के बिक्री और खर्च का सार बताइए।"

