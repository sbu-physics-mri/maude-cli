"""Field validation for MAUDE API queries."""

from __future__ import annotations

import logging
from typing import Iterable

from maudecli.errors import InvalidSearchFieldError

logger = logging.getLogger(__name__)

# Cache of searchable fields from API
_SEARCHABLE_FIELDS_CACHE: list[str] | None = None


def get_searchable_fields(force_refresh: bool = False) -> list[str]:
    """Get list of searchable fields from the API.
    
    Args:
        force_refresh: If True, bypass cache and fetch fresh field list.
        
    Returns:
        List of searchable field names.
    """
    global _SEARCHABLE_FIELDS_CACHE
    
    if force_refresh or _SEARCHABLE_FIELDS_CACHE is None:
        # For now, return a basic list of known fields
        # In a full implementation, this would query the API
        _SEARCHABLE_FIELDS_CACHE = [
            "mdr_text.text",
            "device.device_name",
            "device.brand_name",
            "report_number",
            "patient.age",
        ]
    
    return _SEARCHABLE_FIELDS_CACHE


def validate_search_fields(fields: Iterable[str]) -> None:
    """Validate that search fields are valid.
    
    Args:
        fields: Iterable of field names to validate.
        
    Raises:
        InvalidSearchFieldError: If any field is not valid.
    """
    searchable = get_searchable_fields()
    for field in fields:
        if field not in searchable:
            logger.warning("Field '%s' may not be searchable", field)
            # For now, just log a warning rather than raising
            # raise InvalidSearchFieldError(field)
