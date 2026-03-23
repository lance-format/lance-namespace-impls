"""
List-tables pagination aligned with Java ``PageUtil`` (Hive2 ``listTables``).
"""

from __future__ import annotations

from typing import List, Optional, Tuple

DEFAULT_LIST_TABLES_PAGE_SIZE = 20


def normalize_list_tables_page_size(limit: Optional[int]) -> int:
    if limit is None or limit <= 0:
        return DEFAULT_LIST_TABLES_PAGE_SIZE
    return limit


def split_list_tables_page(
    items: List[str], page_token: Optional[str], page_size: int
) -> Tuple[List[str], Optional[str]]:
    """Slice sorted identifiers using the same rules as Java ``PageUtil.splitPage``."""
    start_index = 0
    if page_token:
        try:
            start_index = int(page_token)
        except ValueError:
            start_index = 0

    if start_index >= len(items):
        return [], None

    end_index = min(start_index + page_size, len(items))
    page_items = items[start_index:end_index]
    next_token = str(end_index) if end_index < len(items) else None
    return page_items, next_token


def apply_list_tables_pagination(
    sorted_table_names: List[str],
    page_token: Optional[str],
    limit: Optional[int],
) -> Tuple[List[str], Optional[str]]:
    """Apply default page size and token slicing (Hive2 ``listTables``)."""
    page_size = normalize_list_tables_page_size(limit)
    return split_list_tables_page(sorted_table_names, page_token, page_size)
