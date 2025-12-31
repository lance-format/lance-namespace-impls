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
package org.lance.namespace.iceberg;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/** Models for Iceberg REST Catalog API requests and responses. */
public class IcebergModels {

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class CreateNamespaceRequest {
    @JsonProperty("namespace")
    private List<String> namespace;

    @JsonProperty("properties")
    private Map<String, String> properties;

    public List<String> getNamespace() {
      return namespace;
    }

    public void setNamespace(List<String> namespace) {
      this.namespace = namespace;
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
    @JsonProperty("namespace")
    private List<String> namespace;

    @JsonProperty("properties")
    private Map<String, String> properties;

    public List<String> getNamespace() {
      return namespace;
    }

    public void setNamespace(List<String> namespace) {
      this.namespace = namespace;
    }

    public Map<String, String> getProperties() {
      return properties;
    }

    public void setProperties(Map<String, String> properties) {
      this.properties = properties;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class GetNamespaceResponse {
    @JsonProperty("namespace")
    private List<String> namespace;

    @JsonProperty("properties")
    private Map<String, String> properties;

    public List<String> getNamespace() {
      return namespace;
    }

    public void setNamespace(List<String> namespace) {
      this.namespace = namespace;
    }

    public Map<String, String> getProperties() {
      return properties;
    }

    public void setProperties(Map<String, String> properties) {
      this.properties = properties;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ListNamespacesResponse {
    @JsonProperty("namespaces")
    private List<List<String>> namespaces;

    @JsonProperty("next-page-token")
    private String nextPageToken;

    public List<List<String>> getNamespaces() {
      return namespaces;
    }

    public void setNamespaces(List<List<String>> namespaces) {
      this.namespaces = namespaces;
    }

    public String getNextPageToken() {
      return nextPageToken;
    }

    public void setNextPageToken(String nextPageToken) {
      this.nextPageToken = nextPageToken;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class TableIdentifier {
    @JsonProperty("namespace")
    private List<String> namespace;

    @JsonProperty("name")
    private String name;

    public List<String> getNamespace() {
      return namespace;
    }

    public void setNamespace(List<String> namespace) {
      this.namespace = namespace;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ListTablesResponse {
    @JsonProperty("identifiers")
    private List<TableIdentifier> identifiers;

    @JsonProperty("next-page-token")
    private String nextPageToken;

    public List<TableIdentifier> getIdentifiers() {
      return identifiers;
    }

    public void setIdentifiers(List<TableIdentifier> identifiers) {
      this.identifiers = identifiers;
    }

    public String getNextPageToken() {
      return nextPageToken;
    }

    public void setNextPageToken(String nextPageToken) {
      this.nextPageToken = nextPageToken;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class IcebergSchema {
    @JsonProperty("type")
    private String type = "struct";

    @JsonProperty("schema-id")
    private Integer schemaId;

    @JsonProperty("fields")
    private List<IcebergField> fields;

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public Integer getSchemaId() {
      return schemaId;
    }

    public void setSchemaId(Integer schemaId) {
      this.schemaId = schemaId;
    }

    public List<IcebergField> getFields() {
      return fields;
    }

    public void setFields(List<IcebergField> fields) {
      this.fields = fields;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class IcebergField {
    @JsonProperty("id")
    private Integer id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("required")
    private Boolean required;

    @JsonProperty("type")
    private String type;

    public Integer getId() {
      return id;
    }

    public void setId(Integer id) {
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Boolean getRequired() {
      return required;
    }

    public void setRequired(Boolean required) {
      this.required = required;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class CreateTableRequest {
    @JsonProperty("name")
    private String name;

    @JsonProperty("location")
    private String location;

    @JsonProperty("schema")
    private IcebergSchema schema;

    @JsonProperty("properties")
    private Map<String, String> properties;

    @JsonProperty("stage-create")
    private Boolean stageCreate;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getLocation() {
      return location;
    }

    public void setLocation(String location) {
      this.location = location;
    }

    public IcebergSchema getSchema() {
      return schema;
    }

    public void setSchema(IcebergSchema schema) {
      this.schema = schema;
    }

    public Map<String, String> getProperties() {
      return properties;
    }

    public void setProperties(Map<String, String> properties) {
      this.properties = properties;
    }

    public Boolean getStageCreate() {
      return stageCreate;
    }

    public void setStageCreate(Boolean stageCreate) {
      this.stageCreate = stageCreate;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class TableMetadata {
    @JsonProperty("format-version")
    private Integer formatVersion;

    @JsonProperty("table-uuid")
    private String tableUuid;

    @JsonProperty("location")
    private String location;

    @JsonProperty("properties")
    private Map<String, String> properties;

    @JsonProperty("schemas")
    private List<IcebergSchema> schemas;

    @JsonProperty("current-schema-id")
    private Integer currentSchemaId;

    public Integer getFormatVersion() {
      return formatVersion;
    }

    public void setFormatVersion(Integer formatVersion) {
      this.formatVersion = formatVersion;
    }

    public String getTableUuid() {
      return tableUuid;
    }

    public void setTableUuid(String tableUuid) {
      this.tableUuid = tableUuid;
    }

    public String getLocation() {
      return location;
    }

    public void setLocation(String location) {
      this.location = location;
    }

    public Map<String, String> getProperties() {
      return properties;
    }

    public void setProperties(Map<String, String> properties) {
      this.properties = properties;
    }

    public List<IcebergSchema> getSchemas() {
      return schemas;
    }

    public void setSchemas(List<IcebergSchema> schemas) {
      this.schemas = schemas;
    }

    public Integer getCurrentSchemaId() {
      return currentSchemaId;
    }

    public void setCurrentSchemaId(Integer currentSchemaId) {
      this.currentSchemaId = currentSchemaId;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class LoadTableResponse {
    @JsonProperty("metadata-location")
    private String metadataLocation;

    @JsonProperty("metadata")
    private TableMetadata metadata;

    @JsonProperty("config")
    private Map<String, String> config;

    public String getMetadataLocation() {
      return metadataLocation;
    }

    public void setMetadataLocation(String metadataLocation) {
      this.metadataLocation = metadataLocation;
    }

    public TableMetadata getMetadata() {
      return metadata;
    }

    public void setMetadata(TableMetadata metadata) {
      this.metadata = metadata;
    }

    public Map<String, String> getConfig() {
      return config;
    }

    public void setConfig(Map<String, String> config) {
      this.config = config;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ConfigResponse {
    @JsonProperty("defaults")
    private Map<String, String> defaults;

    @JsonProperty("overrides")
    private Map<String, String> overrides;

    public Map<String, String> getDefaults() {
      return defaults;
    }

    public void setDefaults(Map<String, String> defaults) {
      this.defaults = defaults;
    }

    public Map<String, String> getOverrides() {
      return overrides;
    }

    public void setOverrides(Map<String, String> overrides) {
      this.overrides = overrides;
    }
  }

  public static IcebergSchema createDummySchema() {
    IcebergSchema schema = new IcebergSchema();
    schema.setType("struct");
    schema.setSchemaId(0);

    IcebergField dummyField = new IcebergField();
    dummyField.setId(1);
    dummyField.setName("dummy");
    dummyField.setRequired(false);
    dummyField.setType("string");

    schema.setFields(java.util.Collections.singletonList(dummyField));
    return schema;
  }
}
