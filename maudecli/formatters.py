"""Output formatters."""

from __future__ import annotations

# Python imports
import logging

logger = logging.getLogger(__name__)


def _format(k: str, v: str  | dict | list, root: str = "") -> str:
    # Try v is a dictionary
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

    try:
        return "".join(
            _format(_k, _v, root=_root)
            for _k, _v in v.items()
        )
    # Try v as a list
    except AttributeError:
        return "".join(
            _format(k, item, root=root + f"_{i}" if i else root)
            for i, item in enumerate(v)
        )


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
