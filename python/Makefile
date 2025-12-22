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

.PHONY: install
install:
	uv pip install -e ".[all,dev]"

.PHONY: install-glue
install-glue:
	uv pip install -e ".[glue,dev]"

.PHONY: install-hive2
install-hive2:
	uv pip install -e ".[hive2,dev]"

.PHONY: test
test:
	uv run pytest tests/

.PHONY: test-glue
test-glue:
	uv run pytest tests/test_glue.py

.PHONY: test-hive
test-hive:
	uv run pytest tests/test_hive.py

.PHONY: test-unity
test-unity:
	uv run pytest tests/test_unity.py

.PHONY: clean
clean:
	rm -rf dist/
	rm -rf build/
	rm -rf *.egg-info
	rm -rf src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

.PHONY: build
build: install
	uv build

.PHONY: publish
publish:
	uv build
