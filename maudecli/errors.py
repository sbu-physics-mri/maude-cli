"""Error classes used in the maude-cli."""

# ruff: noqa: ANN401

from __future__ import annotations

from typing import Any


class CantConvertToStringError(TypeError):
    """Object cannot be converted to a string."""

    def __init__(self, obj: Any) -> None:
        """Initialise the error."""
        super().__init__(
            f"Object of type {type(obj)} can not be converted to a string",
        )

class APIRateLimitError(Exception):
    """API rate limit has been exceeded."""

    def __init__(self, reset_time: int | None = None) -> None:
        """Initialise the error."""
        message = "API rate limit exceeded"
        if reset_time is not None:
            message += f". Rate limit resets in {reset_time} seconds"
        super().__init__(message)
        self.reset_time = reset_time


class APIConnectionError(Exception):
    """Error connecting to the API."""

    def __init__(self, reason: str) -> None:
        """Initialise the error."""
        super().__init__(f"Failed to connect to API: {reason}")
        self.reason = reason


class APIResponseError(Exception):
    """Error response from the API."""

    def __init__(self, status_code: int, message: str) -> None:
        """Initialise the error."""
        super().__init__(f"API returned error {status_code}: {message}")
        self.status_code = status_code
        self.api_message = message


class InvalidSearchFieldError(ValueError):
    """Invalid search field specified."""

    def __init__(self, field: str) -> None:
        """Initialise the error."""
        super().__init__(f"Invalid search field: '{field}'")
        self.field = field
