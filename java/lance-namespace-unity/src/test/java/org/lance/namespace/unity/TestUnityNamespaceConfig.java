/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.namespace.unity;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestUnityNamespaceConfig {

  @Test
  public void testDefaultTimeoutsUseMilliseconds() {
    UnityNamespaceConfig config = new UnityNamespaceConfig(requiredProperties());

    assertThat(config.getConnectTimeout()).isEqualTo(10000);
    assertThat(config.getReadTimeout()).isEqualTo(60000);
  }

  @Test
  public void testCustomTimeoutsUseMilliseconds() {
    Map<String, String> properties = requiredProperties();
    properties.put("connect_timeout", "5000");
    properties.put("read_timeout", "45000");

    UnityNamespaceConfig config = new UnityNamespaceConfig(properties);

    assertThat(config.getConnectTimeout()).isEqualTo(5000);
    assertThat(config.getReadTimeout()).isEqualTo(45000);
  }

  private Map<String, String> requiredProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("endpoint", "http://localhost:8080");
    properties.put("catalog", "unity");
    return properties;
  }
}
