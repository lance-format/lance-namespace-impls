# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""
Unit tests for Hive2 list-tables pagination (Java PageUtil parity).

Loaded via file path so the suite does not import ``lance_namespace_impls`` package
``__init__`` (which pulls in Lance / JVM-dependent modules).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_hive2_page_util():
    root = Path(__file__).resolve().parents[1]
    path = root / "src/lance_namespace_impls/hive2_page_util.py"
    spec = importlib.util.spec_from_file_location("hive2_page_util", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


pu = _load_hive2_page_util()


class TestHive2PageUtil:
    def test_normalize_page_size(self):
        assert pu.normalize_list_tables_page_size(None) == pu.DEFAULT_LIST_TABLES_PAGE_SIZE
        assert pu.normalize_list_tables_page_size(0) == pu.DEFAULT_LIST_TABLES_PAGE_SIZE
        assert pu.normalize_list_tables_page_size(-1) == pu.DEFAULT_LIST_TABLES_PAGE_SIZE
        assert pu.normalize_list_tables_page_size(50) == 50

    def test_split_page_first_and_next_token(self):
        items = ["a", "b", "c", "d"]
        page, token = pu.split_list_tables_page(items, None, 2)
        assert page == ["a", "b"]
        assert token == "2"

        page2, token2 = pu.split_list_tables_page(items, "2", 2)
        assert page2 == ["c", "d"]
        assert token2 is None

    def test_split_page_invalid_token_resets_to_start(self):
        items = ["x", "y"]
        page, token = pu.split_list_tables_page(items, "not-an-int", 1)
        assert page == ["x"]
        assert token == "1"

    def test_split_page_start_beyond_end(self):
        page, token = pu.split_list_tables_page(["only"], "99", 10)
        assert page == []
        assert token is None

    def test_apply_pagination_multi_page(self):
        names = [f"t{i:02d}" for i in range(5)]
        r1 = pu.apply_list_tables_pagination(names, None, 2)
        assert r1 == (["t00", "t01"], "2")

        r2 = pu.apply_list_tables_pagination(names, "2", 2)
        assert r2 == (["t02", "t03"], "4")

        r3 = pu.apply_list_tables_pagination(names, "4", 2)
        assert r3 == (["t04"], None)

    def test_default_limit_full_single_page(self):
        names = ["a", "b"]
        page, token = pu.apply_list_tables_pagination(names, None, None)
        assert page == names
        assert token is None
