from app.utils.app_utils import FastApiApp

# Instantiate the FastAPIApp and get the app instance
app_instance = FastApiApp(version="1.0.0")
app = app_instance.app

# Main entry point for running with Uvicorn (for local development)
if __name__ == "__main__":
    app_instance.run()
