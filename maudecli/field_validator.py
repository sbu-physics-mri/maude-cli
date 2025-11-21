"""Validation and caching of OpenFDA searchable fields."""

from __future__ import annotations

import json
import logging
import time
import urllib.request
from difflib import get_close_matches
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

# Default cache location
CACHE_DIR = Path.home() / ".cache" / "maude-cli"
CACHE_FILE = CACHE_DIR / "searchable_fields.json"
CACHE_EXPIRY_DAYS = 7

# Static fallback list of known searchable fields from OpenFDA documentation
# https://open.fda.gov/apis/device/event/searchable-fields/
KNOWN_FIELDS = [
    "date_facility_aware",
    "date_manufacturer_received",
    "date_of_event",
    "date_received",
    "date_report",
    "date_report_to_fda",
    "date_report_to_manufacturer",
    "device.baseline_510_k__number",
    "device.baseline_510_k__flag",
    "device.baseline_brand_name",
    "device.baseline_catalog_number",
    "device.baseline_date_ceased_marketing",
    "device.baseline_date_first_marketed",
    "device.baseline_device_family",
    "device.baseline_generic_name",
    "device.baseline_model_number",
    "device.baseline_pma_number",
    "device.baseline_pma_flag",
    "device.baseline_shelf_life_contained",
    "device.baseline_shelf_life_in_months",
    "device.brand_name",
    "device.catalog_number",
    "device.date_received",
    "device.date_removed_flag",
    "device.date_returned_to_manufacturer",
    "device.device_age_text",
    "device.device_availability",
    "device.device_date_of_manufacturer",
    "device.device_event_key",
    "device.device_name",
    "device.device_operator",
    "device.device_report_product_code",
    "device.device_sequence_number",
    "device.expiration_date_of_device",
    "device.generic_name",
    "device.implant_flag",
    "device.lot_number",
    "device.manufacturer_d_address_1",
    "device.manufacturer_d_address_2",
    "device.manufacturer_d_city",
    "device.manufacturer_d_country",
    "device.manufacturer_d_name",
    "device.manufacturer_d_postal_code",
    "device.manufacturer_d_state",
    "device.manufacturer_d_zip_code",
    "device.manufacturer_d_zip_code_ext",
    "device.model_number",
    "device.other_id_number",
    "device.device_sequence_no",
    "event_key",
    "event_location",
    "event_type",
    "health_professional",
    "initial_report_to_fda",
    "manufacturer_city",
    "manufacturer_contact_address_1",
    "manufacturer_contact_address_2",
    "manufacturer_contact_area_code",
    "manufacturer_contact_city",
    "manufacturer_contact_country",
    "manufacturer_contact_exchange",
    "manufacturer_contact_extension",
    "manufacturer_contact_f_name",
    "manufacturer_contact_l_name",
    "manufacturer_contact_pcity",
    "manufacturer_contact_pcountry",
    "manufacturer_contact_phone_number",
    "manufacturer_contact_plocal",
    "manufacturer_contact_postal_code",
    "manufacturer_contact_state",
    "manufacturer_contact_t_name",
    "manufacturer_contact_zip_code",
    "manufacturer_contact_zip_ext",
    "manufacturer_country",
    "manufacturer_g1_address_1",
    "manufacturer_g1_address_2",
    "manufacturer_g1_city",
    "manufacturer_g1_country",
    "manufacturer_g1_name",
    "manufacturer_g1_postal_code",
    "manufacturer_g1_state",
    "manufacturer_g1_zip_code",
    "manufacturer_g1_zip_code_ext",
    "manufacturer_link_flag",
    "manufacturer_name",
    "manufacturer_postal_code",
    "manufacturer_state",
    "manufacturer_zip_code",
    "manufacturer_zip_code_ext",
    "mdr_report_key",
    "mdr_text.mdr_text_key",
    "mdr_text.patient_sequence_number",
    "mdr_text.text",
    "mdr_text.text_type_code",
    "number_devices_in_event",
    "number_patients_in_event",
    "patient.date_received",
    "patient.patient_sequence_number",
    "patient.sequence_number_outcome",
    "patient.sequence_number_treatment",
    "previous_use_code",
    "product_problem_flag",
    "product_problems",
    "remedial_action",
    "removal_correction_number",
    "report_number",
    "report_source_code",
    "report_to_fda",
    "report_to_manufacturer",
    "reporter_occupation_code",
    "reprocessed_and_reused_flag",
    "single_use_flag",
    "source_type",
    "type_of_report",
]


def _load_cache() -> dict | None:
    """Load cached searchable fields if available and not expired.
    
    Returns:
        Dict with 'timestamp' and 'fields' keys, or None if cache is invalid/expired.
    """
    if not CACHE_FILE.exists():
        return None
    
    try:
        with CACHE_FILE.open("r") as f:
            cache = json.load(f)
        
        # Check if cache is expired
        cache_age_days = (time.time() - cache["timestamp"]) / (24 * 3600)
        if cache_age_days > CACHE_EXPIRY_DAYS:
            logger.info("Field cache expired (%.1f days old)", cache_age_days)
            return None
        
        logger.debug("Loaded %d fields from cache", len(cache["fields"]))
        return cache
    except (json.JSONDecodeError, KeyError, OSError) as e:
        logger.warning("Failed to load field cache: %s", e)
        return None


def _save_cache(fields: list[str]) -> None:
    """Save searchable fields to cache.
    
    Args:
        fields: List of valid searchable field names.
    """
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache = {
            "timestamp": time.time(),
            "fields": sorted(fields),
        }
        with CACHE_FILE.open("w") as f:
            json.dump(cache, f, indent=2)
        logger.debug("Saved %d fields to cache", len(fields))
    except OSError as e:
        logger.warning("Failed to save field cache: %s", e)


def _fetch_fields_from_api() -> list[str] | None:
    """Fetch searchable fields by querying OpenFDA API for a sample record.
    
    Returns:
        List of field paths, or None if fetching fails.
    """
    try:
        url = "https://api.fda.gov/device/event.json?limit=1"
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())
            
            if "results" not in data or not data["results"]:
                logger.warning("No results in API response")
                return None
            
            result = data["results"][0]
            
            # Recursively extract field paths
            def get_field_paths(obj: dict | list, prefix: str = "") -> list[str]:
                paths = []
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        new_prefix = f"{prefix}.{key}" if prefix else key
                        paths.append(new_prefix)
                        if isinstance(value, (dict, list)):
                            paths.extend(get_field_paths(value, new_prefix))
                elif isinstance(obj, list) and obj:
                    # For lists, use the first item to discover nested fields
                    paths.extend(get_field_paths(obj[0], prefix))
                return paths
            
            fields = sorted(set(get_field_paths(result)))
            logger.info("Fetched %d fields from API", len(fields))
            return fields
            
    except (urllib.error.URLError, json.JSONDecodeError, KeyError) as e:
        logger.warning("Failed to fetch fields from API: %s", e)
        return None


def get_valid_fields() -> list[str]:
    """Get list of valid searchable fields, using cache or fetching from API.
    
    Returns:
        List of valid field names.
    """
    # Try loading from cache first
    cache = _load_cache()
    if cache is not None:
        return cache["fields"]
    
    # Try fetching from API
    logger.info("Fetching searchable fields from OpenFDA API...")
    fields = _fetch_fields_from_api()
    
    # Fall back to known fields if API fails
    if fields is None:
        logger.info("Using static list of %d known fields", len(KNOWN_FIELDS))
        fields = KNOWN_FIELDS.copy()
    
    # Save to cache
    _save_cache(fields)
    
    return fields


def validate_field(field: str, *, suggest_on_error: bool = True) -> tuple[bool, str | None]:
    """Validate a search field and optionally suggest corrections.
    
    Args:
        field: The field name to validate.
        suggest_on_error: If True, suggest similar field names on validation failure.
    
    Returns:
        Tuple of (is_valid, suggestion). If valid, suggestion is None.
        If invalid and suggest_on_error is True, suggestion contains a similar field name.
    """
    valid_fields = get_valid_fields()
    
    if field in valid_fields:
        return True, None
    
    # Field is invalid
    if not suggest_on_error:
        return False, None
    
    # Find similar fields
    suggestions = get_close_matches(field, valid_fields, n=1, cutoff=0.6)
    suggestion = suggestions[0] if suggestions else None
    
    return False, suggestion


def validate_fields(fields: str | Sequence[str]) -> tuple[bool, dict[str, str | None]]:
    """Validate one or more search fields.
    
    Args:
        fields: A single field or comma-separated fields string, or sequence of fields.
    
    Returns:
        Tuple of (all_valid, suggestions_dict).
        suggestions_dict maps invalid field names to suggested corrections (or None).
    """
    # Parse fields
    if isinstance(fields, str):
        field_list = [f.strip() for f in fields.split(",")]
    else:
        field_list = list(fields)
    
    suggestions = {}
    all_valid = True
    
    for field in field_list:
        is_valid, suggestion = validate_field(field, suggest_on_error=True)
        if not is_valid:
            all_valid = False
            suggestions[field] = suggestion
    
    return all_valid, suggestions


def refresh_cache() -> int:
    """Force refresh of the field cache from API.
    
    Returns:
        Number of fields in the refreshed cache.
    """
    logger.info("Force refreshing field cache...")
    fields = _fetch_fields_from_api()
    
    if fields is None:
        logger.warning("Failed to refresh cache, using known fields")
        fields = KNOWN_FIELDS.copy()
    
    _save_cache(fields)
    return len(fields)
