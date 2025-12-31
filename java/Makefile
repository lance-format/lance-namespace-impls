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
	./mvnw spotless:check -pl lance-namespace-glue

.PHONY: build-glue
build-glue:
	./mvnw spotless:apply -pl lance-namespace-glue -am
	./mvnw install -pl lance-namespace-glue -am -DskipTests

.PHONY: test-glue
test-glue:
	./mvnw test -pl lance-namespace-glue

# ============================================================================
# Hive2
# ============================================================================

.PHONY: lint-hive2
lint-hive2:
	./mvnw spotless:check -pl lance-namespace-hive2

.PHONY: build-hive2
build-hive2:
	./mvnw spotless:apply -pl lance-namespace-hive2 -am
	./mvnw install -pl lance-namespace-hive2 -am -DskipTests

.PHONY: test-hive2
test-hive2:
	./mvnw test -pl lance-namespace-hive2 -Dtest="!*IntegrationTest"

# ============================================================================
# Hive3
# ============================================================================

.PHONY: lint-hive3
lint-hive3:
	./mvnw spotless:check -pl lance-namespace-hive3

.PHONY: build-hive3
build-hive3:
	./mvnw spotless:apply -pl lance-namespace-hive3 -am
	./mvnw install -pl lance-namespace-hive3 -am -DskipTests

.PHONY: test-hive3
test-hive3:
	./mvnw test -pl lance-namespace-hive3 -Dtest="!*IntegrationTest"

# ============================================================================
# Iceberg
# ============================================================================

.PHONY: lint-iceberg
lint-iceberg:
	./mvnw spotless:check -pl lance-namespace-iceberg

.PHONY: build-iceberg
build-iceberg:
	./mvnw spotless:apply -pl lance-namespace-iceberg -am
	./mvnw install -pl lance-namespace-iceberg -am -DskipTests

.PHONY: test-iceberg
test-iceberg:
	./mvnw test -pl lance-namespace-iceberg

# ============================================================================
# Polaris
# ============================================================================

.PHONY: lint-polaris
lint-polaris:
	./mvnw spotless:check -pl lance-namespace-polaris

.PHONY: build-polaris
build-polaris:
	./mvnw spotless:apply -pl lance-namespace-polaris -am
	./mvnw install -pl lance-namespace-polaris -am -DskipTests

.PHONY: test-polaris
test-polaris:
	./mvnw test -pl lance-namespace-polaris -Dtest="!*IntegrationTest"

# ============================================================================
# Unity
# ============================================================================

.PHONY: lint-unity
lint-unity:
	./mvnw spotless:check -pl lance-namespace-unity

.PHONY: build-unity
build-unity:
	./mvnw spotless:apply -pl lance-namespace-unity -am
	./mvnw install -pl lance-namespace-unity -am -DskipTests

.PHONY: test-unity
test-unity:
	./mvnw test -pl lance-namespace-unity -Dtest="!*IntegrationTest"

# ============================================================================
# All
# ============================================================================

.PHONY: lint
lint:
	./mvnw spotless:check

.PHONY: clean
clean:
	./mvnw clean

.PHONY: build
build:
	./mvnw spotless:apply
	./mvnw install -DskipTests

.PHONY: test
test:
	./mvnw test

# ============================================================================
# Integration tests
# ============================================================================

.PHONY: integ-test
integ-test:
	./mvnw test -Dtest="*IntegrationTest" -DfailIfNoTests=false

.PHONY: integ-test-hive2
integ-test-hive2:
	./mvnw test -pl lance-namespace-hive2 -Dtest="*IntegrationTest" -DfailIfNoTests=false

.PHONY: integ-test-hive3
integ-test-hive3:
	./mvnw test -pl lance-namespace-hive3 -Dtest="*IntegrationTest" -DfailIfNoTests=false

.PHONY: integ-test-polaris
integ-test-polaris:
	./mvnw test -pl lance-namespace-polaris -Dtest="*Integration" -DfailIfNoTests=false

.PHONY: integ-test-unity
integ-test-unity:
	./mvnw test -pl lance-namespace-unity -Dtest="*IntegrationTest" -DfailIfNoTests=false
