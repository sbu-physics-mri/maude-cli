"""Output formatters."""

from __future__ import annotations

# Python imports
import csv
import logging
import re
from collections.abc import Sequence
from io import StringIO
from typing import Any

logger = logging.getLogger(__name__)


def _format(
    k: str, v: str | dict[str, Any] | list[str], root: str = "",
) -> str:
    logger.debug(
        "Formatting %s with root %s and value %s (type=%s)",
        k,
        root,
        v,
        type(v),
    )
    _root = f"{root}_{k.upper()}" if root else k.upper()
    if isinstance(v, str):
        return f"\n:{_root}: {v}"

    # Explicitly check types instead of using try/except
    if isinstance(v, dict):
        return "".join(
            _format(_k, _v, root=_root)
            for _k, _v in v.items()
        )

    if isinstance(v, Sequence):
        return "".join(
            _format(k, item, root=root + f"_{i}" if i else root)
            for i, item in enumerate(v)
        )

    msg = f"Unsupported type: {type(v)}"
    logger.critical(
        "%s. Supported types are str, dict and Sequence",
        msg,
    )
    raise TypeError(msg)


def as_org(
    results: list[dict],
    name: str = "report_number",
    fields: list | None = None,
    level: int = 3,
) -> str:
  """Format the response as an org-mode todo list.

  Args:
    results : A list of results dictionaries from the JSON API output.
    name : Field to use as the item name in the org output.
        Defaults to "report_number".
    fields : A subset of fields to include. If None (default) then will
        include all fields.
    level : Org heading level - the number of '*' to prepend to the item.
        Defaults to 3.

  Returns:
    out : Formatted org-mode to-do list.

  """
  out = ""
  for r in results:
    name_str = r[name]
    out += "*" * level + " TODO " + name_str + "\n:PROPERTIES:"
    for k, v in r.items():
        if k != name and (fields is None or k in fields):
            out += _format(k, v)
    out += "\n:END:\n"
  return out


def as_csv(results: list[dict], fields: list | None = None) -> str:
    """Format the response as CSV.

    Args:
        results: A list of results dictionaries from the JSON API output.
        fields: A subset of fields to include. If None (default) then will
            include all fields found in the first result.

    Returns:
        out: Formatted CSV string.

    """
    if not results:
        return ""

    # Determine headers
    if fields is None:
        h: set[str] = set()
        for r in results:
            h.update(r.keys())
        headers = sorted(h)
    else:
        headers = fields

    # Create CSV in memory
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)

    writer.writeheader()
    for r in results:
        # Filter to only include specified fields
        row = {k: v for k, v in r.items() if k in headers}
        writer.writerow(row)

    return re.sub("\r", "", output.getvalue())
