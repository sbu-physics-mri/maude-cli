"""Utility functions for maude-cli"""

# Python imports
import hashlib
from pathlib import Path


def compute_file_hash(file_path: Path) -> str:
    """Compute SHA256 hash of a file for content-based deduplication.

    Args:
        file_path: Path to the file.

    Returns:
        Hexadecimal string of the file hash.

    """
    sha256 = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


