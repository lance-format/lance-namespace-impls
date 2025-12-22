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

.PHONY: clean-python
clean-python:
	cd python; make clean

.PHONY: build-python
build-python:
	cd python; make build

.PHONY: test-python
test-python:
	cd python; make test

.PHONY: clean-java
clean-java:
	cd java; make clean

.PHONY: build-java
build-java:
	cd java; make build

.PHONY: test-java
test-java:
	cd java; make test

.PHONY: build-docs
build-docs:
	cd docs; make build

.PHONY: serve-docs
serve-docs:
	cd docs; make serve

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
# Integration test targets
# ============================================================================

.PHONY: integration-test-java
integration-test-java:
	cd java && ./mvnw test -Dtest="*IntegrationTest" -DfailIfNoTests=false

.PHONY: integration-test-hive2
integration-test-hive2:
	cd java && ./mvnw test -pl lance-namespace-hive2 -Dtest="*IntegrationTest" -DfailIfNoTests=false

.PHONY: integration-test-hive3
integration-test-hive3:
	cd java && ./mvnw test -pl lance-namespace-hive3 -Dtest="*IntegrationTest" -DfailIfNoTests=false

.PHONY: integration-test-polaris
integration-test-polaris:
	cd java && ./mvnw test -pl lance-namespace-polaris -Dtest="*IntegrationTest" -DfailIfNoTests=false

.PHONY: integration-test-unity
integration-test-unity:
	cd java && ./mvnw test -pl lance-namespace-unity -Dtest="*IntegrationTest" -DfailIfNoTests=false

.PHONY: integration-test
integration-test: integration-test-java
