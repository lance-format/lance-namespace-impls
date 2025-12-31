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
# Python
# ============================================================================

.PHONY: clean-python
clean-python:
	cd python && make clean

.PHONY: build-python
build-python:
	cd python && make build

.PHONY: test-python
test-python:
	cd python && make test

# ============================================================================
# Java
# ============================================================================

.PHONY: clean-java
clean-java:
	cd java && make clean

.PHONY: build-java
build-java:
	cd java && make build

.PHONY: test-java
test-java:
	cd java && make test

# ============================================================================
# Docs
# ============================================================================

.PHONY: build-docs
build-docs:
	cd docs && make build

.PHONY: serve-docs
serve-docs:
	cd docs && make serve

# ============================================================================
# All
# ============================================================================

.PHONY: clean
clean: clean-python clean-java

.PHONY: build
build: build-python build-java

.PHONY: test
test: test-python test-java

# ============================================================================
# Docker targets for integration testing
# ============================================================================

.PHONY: docker-setup
docker-setup:
	cd docker && make setup

.PHONY: docker-up
docker-up: docker-setup
	cd docker && make up

.PHONY: docker-down
docker-down:
	cd docker && make down

.PHONY: docker-down-clean
docker-down-clean:
	cd docker && make down-clean

.PHONY: docker-status
docker-status:
	cd docker && make status

.PHONY: docker-health
docker-health:
	cd docker && make health

.PHONY: docker-logs
docker-logs:
	cd docker && make logs

# Individual catalog docker targets
.PHONY: docker-up-hive2 docker-up-hive3 docker-up-polaris docker-up-unity
docker-up-hive2:
	cd docker && make up-hive2
docker-up-hive3:
	cd docker && make up-hive3
docker-up-polaris:
	cd docker && make up-polaris
docker-up-unity:
	cd docker && make up-unity

.PHONY: docker-down-hive2 docker-down-hive3 docker-down-polaris docker-down-unity
docker-down-hive2:
	cd docker && make down-hive2
docker-down-hive3:
	cd docker && make down-hive3
docker-down-polaris:
	cd docker && make down-polaris
docker-down-unity:
	cd docker && make down-unity

# ============================================================================
# Java Integration test targets
# ============================================================================

.PHONY: java-integ-test
java-integ-test:
	cd java && make integ-test

.PHONY: java-integ-test-hive2
java-integ-test-hive2:
	cd java && make integ-test-hive2

.PHONY: java-integ-test-hive3
java-integ-test-hive3:
	cd java && make integ-test-hive3

.PHONY: java-integ-test-polaris
java-integ-test-polaris:
	cd java && make integ-test-polaris

.PHONY: java-integ-test-unity
java-integ-test-unity:
	cd java && make integ-test-unity

# ============================================================================
# Python Integration test targets
# ============================================================================

.PHONY: python-integ-test
python-integ-test:
	cd python && make integ-test

.PHONY: python-integ-test-hive
python-integ-test-hive:
	cd python && make integ-test-hive

.PHONY: python-integ-test-hive2
python-integ-test-hive2:
	cd python && make integ-test-hive2

.PHONY: python-integ-test-hive3
python-integ-test-hive3:
	cd python && make integ-test-hive3

.PHONY: python-integ-test-polaris
python-integ-test-polaris:
	cd python && make integ-test-polaris

.PHONY: python-integ-test-unity
python-integ-test-unity:
	cd python && make integ-test-unity
