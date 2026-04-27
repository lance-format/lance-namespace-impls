"""
Shared table metadata helpers for Lance Namespace implementations.
"""

from typing import Dict, Optional


def merge_table_properties(
    properties: Optional[Dict[str, str]], required_properties: Dict[str, str]
) -> Dict[str, str]:
    """Merge caller-provided table properties with implementation-required markers."""
    merged = dict(properties or {})
    merged.update(required_properties)
    return merged


def include_declared(include_declared_value: Optional[bool]) -> bool:
    """Return the 0.7.2 ListTables include_declared default."""
    return include_declared_value is not False


def has_storage_components(
    location: Optional[str], storage_options: Optional[Dict[str, str]] = None
) -> bool:
    """Check whether a catalog table location can be opened as a Lance dataset."""
    if not location:
        return False

    try:
        import lance

        dataset = lance.dataset(location, storage_options=storage_options or {})
        close = getattr(dataset, "close", None)
        if close is not None:
            close()
        return True
    except Exception:
        return False


def is_only_declared(
    location: Optional[str], storage_options: Optional[Dict[str, str]] = None
) -> bool:
    """Return true when a catalog table exists but no Lance storage can be opened."""
    return not has_storage_components(location, storage_options)
