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
package org.lance.namespace.gravitino;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/** Models for Gravitino Lance REST API requests and responses. */
public class GravitinoModels {

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ListNamespacesResponse {
    @JsonProperty("namespaces")
    private List<String> namespaces;

    public List<String> getNamespaces() {
      return namespaces;
    }

    public void setNamespaces(List<String> namespaces) {
      this.namespaces = namespaces;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class CreateNamespaceRequest {
    @JsonProperty("id")
    private List<String> id;

    @JsonProperty("mode")
    private String mode;

    @JsonProperty("properties")
    private Map<String, String> properties;

    public List<String> getId() {
      return id;
    }

    public void setId(List<String> id) {
      this.id = id;
    }

    public String getMode() {
      return mode;
    }

    public void setMode(String mode) {
      this.mode = mode;
    }

    public Map<String, String> getProperties() {
      return properties;
    }

    public void setProperties(Map<String, String> properties) {
      this.properties = properties;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class CreateNamespaceResponse {
    @JsonProperty("properties")
    private Map<String, String> properties;

    public Map<String, String> getProperties() {
      return properties;
    }

    public void setProperties(Map<String, String> properties) {
      this.properties = properties;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DescribeNamespaceRequest {
    @JsonProperty("id")
    private List<String> id;

    public List<String> getId() {
      return id;
    }

    public void setId(List<String> id) {
      this.id = id;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DescribeNamespaceResponse {
    @JsonProperty("properties")
    private Map<String, String> properties;

    public Map<String, String> getProperties() {
      return properties;
    }

    public void setProperties(Map<String, String> properties) {
      this.properties = properties;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DropNamespaceRequest {
    @JsonProperty("id")
    private List<String> id;

    @JsonProperty("behavior")
    private String behavior;

    public List<String> getId() {
      return id;
    }

    public void setId(List<String> id) {
      this.id = id;
    }

    public String getBehavior() {
      return behavior;
    }

    public void setBehavior(String behavior) {
      this.behavior = behavior;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ListTablesResponse {
    @JsonProperty("tables")
    private List<String> tables;

    public List<String> getTables() {
      return tables;
    }

    public void setTables(List<String> tables) {
      this.tables = tables;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class RegisterTableRequest {
    @JsonProperty("id")
    private List<String> id;

    @JsonProperty("location")
    private String location;

    @JsonProperty("mode")
    private String mode;

    public List<String> getId() {
      return id;
    }

    public void setId(List<String> id) {
      this.id = id;
    }

    public String getLocation() {
      return location;
    }

    public void setLocation(String location) {
      this.location = location;
    }

    public String getMode() {
      return mode;
    }

    public void setMode(String mode) {
      this.mode = mode;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class RegisterTableResponse {
    @JsonProperty("location")
    private String location;

    public String getLocation() {
      return location;
    }

    public void setLocation(String location) {
      this.location = location;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class TableExistsRequest {
    @JsonProperty("id")
    private List<String> id;

    public List<String> getId() {
      return id;
    }

    public void setId(List<String> id) {
      this.id = id;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class TableExistsResponse {
    @JsonProperty("exists")
    private boolean exists;

    public boolean isExists() {
      return exists;
    }

    public void setExists(boolean exists) {
      this.exists = exists;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DeregisterTableRequest {
    @JsonProperty("id")
    private List<String> id;

    public List<String> getId() {
      return id;
    }

    public void setId(List<String> id) {
      this.id = id;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DeregisterTableResponse {
    @JsonProperty("location")
    private String location;

    public String getLocation() {
      return location;
    }

    public void setLocation(String location) {
      this.location = location;
    }
  }
}
