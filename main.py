"""Application entry point for FastAPI server process."""

from app.utils.app_utils import FastApiApp


def create_application() -> FastApiApp:
    """Create configured FastAPI wrapper instance."""
    return FastApiApp(version="1.0.0")


app_instance = create_application()
app = app_instance.app

if __name__ == "__main__":
    app_instance.run()
