import logging
import secrets
import time
import json
from datetime import datetime
from fastapi import FastAPI, Response
from fastapi.requests import Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from uvicorn import run as uvicorn_run
from decouple import config
from contextlib import asynccontextmanager

from app.core.config import settings
from app.db.session import sqlite_engine,get_db
from app.db.base import SqliteBase
from app.db.models.user import LogData


class FastApiApp:
    """
    A class to create and configure a FastAPI application instance with middleware, routers, and logging.

    This class encapsulates the setup and configuration of the FastAPI app, including:
    - Initialization of logging for structured output.
    - A lifespan context manager to handle startup and shutdown tasks, such as database initialization.
    - Middleware for session management, CORS, security headers, and CSRF protection.
    - Routing of all endpoint modules.

    Attributes:
        CSRF_TOKEN_NAME (str): The name of the CSRF token to be used in session storage.

    Methods:
        __init__(): Initializes the FastAPI application, configures logging, adds middleware, and includes routers.
        _configure_logging(): Configures logging with a structured format.
        _lifespan(app: FastAPI): A context manager for handling startup and shutdown tasks.
        _add_middleware(): Adds necessary middleware for CORS, security headers, session management, and CSRF protection.
        _include_routers(): Includes all endpoint routers in the FastAPI app.
        run(): Runs the FastAPI app using Uvicorn with configurable settings.
    """
    def __init__(self, version: str = "1.0.0"):
        """
        Initializes the FastAPI application by configuring logging, adding middleware, 
        and including routers for all endpoints.

        This method configures logging for structured output, initializes the FastAPI app instance,
        adds necessary middleware for session management, CORS, security headers, and CSRF protection,
        and finally includes the routers for handling API endpoints.
        
        :param version: The version of API
        :type version: str
        """
        self.app_version = version
        self._configure_logging()

        # Initialize FastAPI App
        self.app = FastAPI(
            Title="Traders Family Campaign Data API",
            description="API fro handling Traders Family Campaign Data",
            version=self.app_version,
            docs_url=None if not settings.DEBUG else "/docs",
            redoc_url="/redoc",
            lifespan=self._lifespan
        )

        # Add Middleware
        self._add_middleware()

        # Include Routes
        self._include_routers_v1()

    def _configure_logging(self):
        """
        Configures logging for the FastAPI app.

        Sets up logging to output messages in a structured format with 
        the following attributes:
        - Log level: INFO
        - Format: Timestamp, logger name, log level, and message

        This method ensures that logs are formatted in a consistent way, 
        making it easier to track and analyze logs in production.
        """
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    @asynccontextmanager
    async def _lifespan(self, app: FastAPI):
        """
        A context manager for handling startup and shutdown tasks for the FastAPI app.

        This method handles the following tasks:
        - **Startup**: Initializes the database by creating the necessary tables.
        - **Shutdown**: Disposes of the database connection after the application shuts down.

        It is invoked automatically by FastAPI when the app starts up and shuts down.
        
        :param app: Fast API Application
        :type app: FastAPI
        """
        print("Starting up...")
        async with sqlite_engine.begin() as conn:
            await conn.run_sync(SqliteBase.metadata.create_all)
        yield
        print("Shutting down...")
        await sqlite_engine.dispose()

    def _add_middleware(self):
        """
        Adds necessary middleware to the FastAPI app for security, CORS, session management, and CSRF protection.

        - Configures CORS for different environments (allowing all origins in development or specific origins in production).
        - Adds security headers (e.g., X-Content-Type-Options, X-Frame-Options).
        - Manages sessions and generates CSRF tokens for secure user interaction.

        This method ensures that the app adheres to best practices for security and resource sharing.
        """
        # Add CORS Middleware
        if settings.DEBUG:
            self.app.add_middleware(
                CORSMiddleware,
                allow_origins=["*"],
                allow_credentials=True,
                allow_methods=["*"],
                allow_headers=["*"]
            )
        else:
            self.app.add_middleware(
                CORSMiddleware,
                allow_origins=[config("FRONTEND_URL")],
                allow_credentials=True,
                allow_methods=["GET", "POST"],
                allow_headers=["Authorization", "Content-Type"]
            )

        @self.app.middleware("http")
        async def log_request(request: Request, call_next):
            """
            Middleware to log HTTP request and response details in a FastAPI application.

            This middleware intercepts all incoming HTTP requests and outgoing responses, 
            capturing metrics such as processing time and response content for logging purposes.

            Args:
                request (Request): The incoming HTTP request object.
                call_next (Callable): The next middleware or endpoint to call in the request-response cycle.

            Returns:
                Response: A reconstructed HTTP response with the same content and properties as the original.

            Functionality:
                - Captures the start time of the request processing.
                - Processes the request and captures the original response.
                - Reads the response body by iterating over the response's body iterator.
                - Reconstructs the response with the captured content to ensure the response integrity is maintained.
                - Attempts to parse the response body as JSON; falls back to plain text if parsing fails.
                - Logs the following details:
                    - Request URL
                    - HTTP method
                    - Processing time in seconds
                    - HTTP status code of the response
                    - Response body content (JSON or plain text)
                - Returns the reconstructed response to the client.

            Notes:
                - Handles both JSON and plain-text response bodies.
                - Adds performance overhead due to buffering the response content.
                - Ensure sensitive information is not logged unintentionally.
                - May not be suitable for very large or streamed responses.
            
            :param request: Description
            :type request: Request
            :param call_next: Description
            """
            start_time = time.time()
            
            # Process the request and get the original response
            original_response = await call_next(request)

            # Clone the original response content
            content = b""
            async for chunk in original_response.body_iterator:
                content += chunk

            # Create a new Response with the same content
            new_response = Response(
                content=content,
                status_code=original_response.status_code,
                headers=dict(original_response.headers),
                media_type=original_response.media_type
            )

            # attemp to parse JSON if possible
            try:
                response_body = json.loads(content.decode())
            except json.JSONDecodeError:
                response_body = content.decode()

            process_time = time.time() - start_time

            # Log details
            async_gen = get_db()
            session = await anext(async_gen)

            data = LogData(
                url=str(request.url),
                method=request.method,
                time=process_time,
                status=new_response.status_code,
                response=response_body,
                created_at=datetime.now()
            )

            session.add(data)
            await session.commit()
            await session.close()

            return new_response
        
        # Security headers middleware
        @self.app.middleware("http")
        async def security_headers_middleware(
            request: Request,
            call_next
        ):
            """
            Adds security headers to the HTTP response for enhanced protection.

            The headers added are:
            - X-Content-Type-Options: Prevents MIME type sniffing.
            - X-Frame-Options: Prevents the app from being embedded in iframes.
            - Strict-Transport-Security: Enforces HTTPS connections.
            - Referrer-Policy: Restricts referer information sent in requests.
            - Permissions-Policy: Blocks access to features like geolocation, microphone, and camera.

            This middleware is essential for securing the app from common web vulnerabilities.
            
            :param request: JSON reuquest Form
            :type request: Request
            :param call_next: Caller next JSON response
            """
            response = await call_next(request)
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
            response.headers["Referrer-Policy"] = "same-origin"
            response.headers["Permissions-Policy"] = ("geolocation=(), microphone=(), camera=()")

            return response
        
        # CSRF token middleware
        @self.app.middleware("http")
        async def add_csrf_token_middleware(
            request: Request,
            call_next
        ):
            """
            Generates and attaches a CSRF token to the session and response.

            If a CSRF token is not found in the session, it is generated and added to the session.
            The token is then sent as an HTTP-only cookie to the client to ensure secure transfer.
            
            :param request: JSON request form
            :type request: Request
            :param call_next: JSON caller next response
            """
            CSRF_TOKEN_NAME = "csrf_token"
            
            if CSRF_TOKEN_NAME not in request.session:
                csrf_token = secrets.token_hex(16)
                request.session[CSRF_TOKEN_NAME] = csrf_token

            response = await call_next(request)
            csrf_token = request.session[CSRF_TOKEN_NAME]
            response.set_cookie(
                key=CSRF_TOKEN_NAME,
                value=csrf_token,
                httponly=True,
                secure=True
            )

            return response
        
