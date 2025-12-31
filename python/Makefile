# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ============================================================================
# Glue
# ============================================================================

.PHONY: lint-glue
lint-glue:
	uv run ruff check src/lance_namespace_impls/glue.py tests/test_glue.py
	uv run ruff format --check src/lance_namespace_impls/glue.py tests/test_glue.py

.PHONY: install-glue
install-glue:
	uv sync --extra glue --extra dev

.PHONY: test-glue
test-glue:
	uv run pytest tests/test_glue.py

# ============================================================================
# Hive2
# ============================================================================

.PHONY: lint-hive
lint-hive:
	uv run ruff check src/lance_namespace_impls/hive2.py src/lance_namespace_impls/hive3.py tests/test_hive2.py tests/test_hive3.py tests/test_hive2_integration.py tests/test_hive3_integration.py
	uv run ruff format --check src/lance_namespace_impls/hive2.py src/lance_namespace_impls/hive3.py tests/test_hive2.py tests/test_hive3.py tests/test_hive2_integration.py tests/test_hive3_integration.py

.PHONY: install-hive
install-hive:
	uv sync --extra hive2 --extra hive3 --extra dev

.PHONY: test-hive
test-hive:
	uv run pytest tests/test_hive2.py tests/test_hive3.py

# ============================================================================
# Unity
# ============================================================================

.PHONY: lint-unity
lint-unity:
	uv run ruff check src/lance_namespace_impls/unity.py tests/test_unity.py tests/test_unity_integration.py
	uv run ruff format --check src/lance_namespace_impls/unity.py tests/test_unity.py tests/test_unity_integration.py

.PHONY: install-unity
install-unity:
	uv sync --extra unity --extra dev

.PHONY: test-unity
test-unity:
	uv run pytest tests/test_unity.py

# ============================================================================
# Iceberg
# ============================================================================

.PHONY: lint-iceberg
lint-iceberg:
	uv run ruff check src/lance_namespace_impls/iceberg.py tests/test_iceberg.py tests/test_iceberg_integration.py
	uv run ruff format --check src/lance_namespace_impls/iceberg.py tests/test_iceberg.py tests/test_iceberg_integration.py

.PHONY: install-iceberg
install-iceberg:
	uv sync --extra iceberg --extra dev

.PHONY: test-iceberg
test-iceberg:
	uv run pytest tests/test_iceberg.py

# ============================================================================
# Polaris
# ============================================================================

.PHONY: lint-polaris
lint-polaris:
	uv run ruff check src/lance_namespace_impls/polaris.py tests/test_polaris.py tests/test_polaris_integration.py
	uv run ruff format --check src/lance_namespace_impls/polaris.py tests/test_polaris.py tests/test_polaris_integration.py

.PHONY: install-polaris
install-polaris:
	uv sync --extra polaris --extra dev

.PHONY: test-polaris
test-polaris:
	uv run pytest tests/test_polaris.py

# ============================================================================
# All
# ============================================================================

.PHONY: lint
lint:
	uv run ruff check .
	uv run ruff format --check .

.PHONY: install
install:
	uv sync --extra all --extra dev

.PHONY: test
test:
	uv run pytest tests/

.PHONY: clean
clean:
	rm -rf dist/
	rm -rf build/
	rm -rf *.egg-info
	rm -rf src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

.PHONY: build
build:
	uv build

.PHONY: publish
publish:
	uv build

# ============================================================================
# Integration tests
# ============================================================================

.PHONY: integ-test
integ-test:
	uv run pytest tests/ -m integration

.PHONY: integ-test-hive
integ-test-hive:
	uv run pytest tests/test_hive2_integration.py tests/test_hive3_integration.py -v

.PHONY: integ-test-hive2
integ-test-hive2:
	uv run pytest tests/test_hive2_integration.py -v

.PHONY: integ-test-hive3
integ-test-hive3:
	uv run pytest tests/test_hive3_integration.py -v

.PHONY: integ-test-polaris
integ-test-polaris:
	uv run pytest tests/test_polaris_integration.py -v

.PHONY: integ-test-unity
integ-test-unity:
	uv run pytest tests/test_unity_integration.py -v

.PHONY: integ-test-iceberg
integ-test-iceberg:
	uv run pytest tests/test_iceberg_integration.py -v
