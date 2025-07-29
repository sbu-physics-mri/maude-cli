"""Error classes used in the maude-cli."""

# ruff: noqa: ANN401

from typing import Any


class CantConvertToStringError(TypeError):
    """Object cannot be converted to a string."""

    def __init__(self, obj: Any) -> None:
        """Initialise the error."""
        super().__init__(
            f"Object of type {type(obj)} can not be converted to a string",
        )
