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

.PHONY: build-glue
build-glue:
	./mvnw spotless:apply -pl lance-namespace-glue -am
	./mvnw install -pl lance-namespace-glue -am

.PHONY: build-hive2
build-hive2:
	./mvnw spotless:apply -pl lance-namespace-hive2 -am
	./mvnw install -pl lance-namespace-hive2 -am

.PHONY: build-hive3
build-hive3:
	./mvnw spotless:apply -pl lance-namespace-hive3 -am
	./mvnw install -pl lance-namespace-hive3 -am

.PHONY: build-unity
build-unity:
	./mvnw spotless:apply -pl lance-namespace-unity -am
	./mvnw install -pl lance-namespace-unity -am

.PHONY: build-polaris
build-polaris:
	./mvnw spotless:apply -pl lance-namespace-polaris -am
	./mvnw install -pl lance-namespace-polaris -am

.PHONY: clean
clean:
	./mvnw clean

.PHONY: build
build:
	./mvnw spotless:apply
	./mvnw install

.PHONY: test
test:
	./mvnw test
