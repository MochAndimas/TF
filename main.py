"""Application entry point for FastAPI server process.

This module exposes ASGI app objects (`app_instance`, `app`) used by
Uvicorn/runtime imports and keeps startup wiring centralized.
"""

from app.utils.app_utils import FastApiApp


def create_application() -> FastApiApp:
    """Create configured FastAPI application wrapper.

    Returns:
        FastApiApp: Wrapper containing configured FastAPI app and runtime helpers.
    """
    return FastApiApp(version="1.0.0")


app_instance = create_application()
app = app_instance.app

if __name__ == "__main__":
    app_instance.run()
