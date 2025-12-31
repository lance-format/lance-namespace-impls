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
package org.lance.namespace.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPatch;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A reusable REST client for making HTTP requests to REST APIs.
 *
 * <p>This client provides:
 *
 * <ul>
 *   <li>Connection pooling for efficient HTTP connections
 *   <li>Configurable timeouts for connect and read operations
 *   <li>Retry logic with exponential backoff
 *   <li>JSON serialization/deserialization via Jackson
 *   <li>Support for common HTTP methods (GET, POST, PUT, PATCH, DELETE)
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * RestClient client = RestClient.builder()
 *     .baseUrl("http://localhost:8080/api")
 *     .header("Authorization", "Bearer token")
 *     .connectTimeout(10, TimeUnit.SECONDS)
 *     .readTimeout(30, TimeUnit.SECONDS)
 *     .maxRetries(3)
 *     .build();
 *
 * MyResponse response = client.get("/resource", MyResponse.class);
 * }</pre>
 */
public class RestClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(RestClient.class);

  private static final int DEFAULT_MAX_CONNECTIONS = 20;
  private static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 10;
  private static final int DEFAULT_CONNECT_TIMEOUT_MS = 10000;
  private static final int DEFAULT_READ_TIMEOUT_MS = 30000;
  private static final int DEFAULT_MAX_RETRIES = 3;
  private static final long DEFAULT_RETRY_DELAY_MS = 1000;

  private final String baseUrl;
  private final Map<String, String> defaultHeaders;
  private final CloseableHttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final int maxRetries;
  private final long retryDelayMs;

  private RestClient(Builder builder) {
    this.baseUrl =
        builder.baseUrl.endsWith("/")
            ? builder.baseUrl.substring(0, builder.baseUrl.length() - 1)
            : builder.baseUrl;
    this.defaultHeaders = new HashMap<>(builder.defaultHeaders);
    this.maxRetries = builder.maxRetries;
    this.retryDelayMs = builder.retryDelayMs;

    this.objectMapper =
        builder.objectMapper != null
            ? builder.objectMapper
            : new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(builder.maxConnections);
    connectionManager.setDefaultMaxPerRoute(builder.maxConnectionsPerRoute);

    RequestConfig requestConfig =
        RequestConfig.custom()
            .setConnectTimeout(Timeout.ofMilliseconds(builder.connectTimeoutMs))
            .setResponseTimeout(Timeout.ofMilliseconds(builder.readTimeoutMs))
            .build();

    this.httpClient =
        HttpClients.custom()
            .setConnectionManager(connectionManager)
            .setDefaultRequestConfig(requestConfig)
            .build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  public <T> T get(String path, Class<T> responseType) throws RestClientException {
    return execute(new HttpGet(buildUri(path)), null, responseType);
  }

  public <T> T get(String path, Map<String, String> queryParams, Class<T> responseType)
      throws RestClientException {
    HttpGet request = new HttpGet(buildUri(path, queryParams));
    return execute(request, null, responseType);
  }

  public <T> T getWithHeaders(String path, Map<String, String> headers, Class<T> responseType)
      throws RestClientException {
    HttpGet request = new HttpGet(buildUri(path));
    headers.forEach(request::addHeader);
    return execute(request, null, responseType);
  }

  public <T> T get(
      String path,
      Map<String, String> queryParams,
      Map<String, String> headers,
      Class<T> responseType)
      throws RestClientException {
    HttpGet request = new HttpGet(buildUri(path, queryParams));
    headers.forEach(request::addHeader);
    return execute(request, null, responseType);
  }

  public <T> T post(String path, Object body, Class<T> responseType) throws RestClientException {
    HttpPost request = new HttpPost(buildUri(path));
    return execute(request, body, responseType);
  }

  public <T> T post(String path, Object body, Map<String, String> headers, Class<T> responseType)
      throws RestClientException {
    HttpPost request = new HttpPost(buildUri(path));
    headers.forEach(request::addHeader);
    return execute(request, body, responseType);
  }

  public <T> T put(String path, Object body, Class<T> responseType) throws RestClientException {
    HttpPut request = new HttpPut(buildUri(path));
    return execute(request, body, responseType);
  }

  public <T> T put(String path, Object body, Map<String, String> headers, Class<T> responseType)
      throws RestClientException {
    HttpPut request = new HttpPut(buildUri(path));
    headers.forEach(request::addHeader);
    return execute(request, body, responseType);
  }

  public <T> T patch(String path, Object body, Class<T> responseType) throws RestClientException {
    HttpPatch request = new HttpPatch(buildUri(path));
    return execute(request, body, responseType);
  }

  public void delete(String path) throws RestClientException {
    execute(new HttpDelete(buildUri(path)), null, Void.class);
  }

  public void delete(String path, Map<String, String> headers) throws RestClientException {
    HttpDelete request = new HttpDelete(buildUri(path));
    headers.forEach(request::addHeader);
    execute(request, null, Void.class);
  }

  public <T> T delete(String path, Class<T> responseType) throws RestClientException {
    return execute(new HttpDelete(buildUri(path)), null, responseType);
  }

  private URI buildUri(String path) {
    String fullPath = path.startsWith("/") ? baseUrl + path : baseUrl + "/" + path;
    return URI.create(fullPath);
  }

  private URI buildUri(String path, Map<String, String> queryParams) {
    String fullPath = path.startsWith("/") ? baseUrl + path : baseUrl + "/" + path;
    if (queryParams != null && !queryParams.isEmpty()) {
      StringBuilder sb = new StringBuilder(fullPath);
      sb.append("?");
      boolean first = true;
      for (Map.Entry<String, String> entry : queryParams.entrySet()) {
        if (!first) {
          sb.append("&");
        }
        try {
          sb.append(java.net.URLEncoder.encode(entry.getKey(), "UTF-8"));
          sb.append("=");
          sb.append(java.net.URLEncoder.encode(entry.getValue(), "UTF-8"));
        } catch (java.io.UnsupportedEncodingException e) {
          throw new RuntimeException("UTF-8 encoding not supported", e);
        }
        first = false;
      }
      fullPath = sb.toString();
    }
    return URI.create(fullPath);
  }

  private <T> T execute(HttpUriRequestBase request, Object body, Class<T> responseType)
      throws RestClientException {
    defaultHeaders.forEach(request::addHeader);

    if (body != null) {
      try {
        String jsonBody = objectMapper.writeValueAsString(body);
        request.setEntity(new StringEntity(jsonBody, ContentType.APPLICATION_JSON));
      } catch (JsonProcessingException e) {
        throw new RestClientException(-1, "Failed to serialize request body", e);
      }
    }

    int attempt = 0;
    RestClientException lastException = null;

    while (attempt <= maxRetries) {
      try {
        return httpClient.execute(
            request,
            response -> {
              int statusCode = response.getCode();
              String responseBody =
                  response.getEntity() != null ? EntityUtils.toString(response.getEntity()) : null;

              if (statusCode >= 200 && statusCode < 300) {
                if (responseType == Void.class || responseBody == null || responseBody.isEmpty()) {
                  return null;
                }
                try {
                  return objectMapper.readValue(responseBody, responseType);
                } catch (JsonProcessingException e) {
                  throw new RestClientException(
                      statusCode, "Failed to deserialize response: " + responseBody, e);
                }
              } else {
                throw new RestClientException(statusCode, responseBody);
              }
            });
      } catch (RestClientException e) {
        lastException = e;
        if (e.getStatusCode() >= 400 && e.getStatusCode() < 500) {
          throw e;
        }
        attempt++;
        if (attempt <= maxRetries) {
          long delay = retryDelayMs * (1L << (attempt - 1));
          LOG.warn(
              "Request failed with status {}, retrying in {}ms (attempt {}/{})",
              e.getStatusCode(),
              delay,
              attempt,
              maxRetries);
          try {
            Thread.sleep(delay);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RestClientException(-1, "Interrupted during retry", ie);
          }
        }
      } catch (IOException e) {
        lastException = new RestClientException(-1, "IO error: " + e.getMessage(), e);
        attempt++;
        if (attempt <= maxRetries) {
          long delay = retryDelayMs * (1L << (attempt - 1));
          LOG.warn(
              "Request failed with IO error, retrying in {}ms (attempt {}/{})",
              delay,
              attempt,
              maxRetries);
          try {
            Thread.sleep(delay);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RestClientException(-1, "Interrupted during retry", ie);
          }
        }
      }
    }

    throw lastException != null
        ? lastException
        : new RestClientException(-1, "Unknown error after retries");
  }

  @Override
  public void close() throws IOException {
    if (httpClient != null) {
      httpClient.close();
    }
  }

  public static class Builder {
    private String baseUrl;
    private final Map<String, String> defaultHeaders = new HashMap<>();
    private int maxConnections = DEFAULT_MAX_CONNECTIONS;
    private int maxConnectionsPerRoute = DEFAULT_MAX_CONNECTIONS_PER_ROUTE;
    private int connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS;
    private int readTimeoutMs = DEFAULT_READ_TIMEOUT_MS;
    private int maxRetries = DEFAULT_MAX_RETRIES;
    private long retryDelayMs = DEFAULT_RETRY_DELAY_MS;
    private ObjectMapper objectMapper;

    public Builder baseUrl(String baseUrl) {
      this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl cannot be null");
      return this;
    }

    public Builder header(String name, String value) {
      this.defaultHeaders.put(name, value);
      return this;
    }

    public Builder headers(Map<String, String> headers) {
      this.defaultHeaders.putAll(headers);
      return this;
    }

    public Builder authToken(String token) {
      if (token != null && !token.isEmpty()) {
        this.defaultHeaders.put("Authorization", "Bearer " + token);
      }
      return this;
    }

    public Builder maxConnections(int maxConnections) {
      this.maxConnections = maxConnections;
      return this;
    }

    public Builder maxConnectionsPerRoute(int maxConnectionsPerRoute) {
      this.maxConnectionsPerRoute = maxConnectionsPerRoute;
      return this;
    }

    public Builder connectTimeout(int timeout, TimeUnit unit) {
      this.connectTimeoutMs = (int) unit.toMillis(timeout);
      return this;
    }

    public Builder connectTimeoutMs(int connectTimeoutMs) {
      this.connectTimeoutMs = connectTimeoutMs;
      return this;
    }

    public Builder readTimeout(int timeout, TimeUnit unit) {
      this.readTimeoutMs = (int) unit.toMillis(timeout);
      return this;
    }

    public Builder readTimeoutMs(int readTimeoutMs) {
      this.readTimeoutMs = readTimeoutMs;
      return this;
    }

    public Builder maxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
    }

    public Builder retryDelay(long delay, TimeUnit unit) {
      this.retryDelayMs = unit.toMillis(delay);
      return this;
    }

    public Builder retryDelayMs(long retryDelayMs) {
      this.retryDelayMs = retryDelayMs;
      return this;
    }

    public Builder objectMapper(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
      return this;
    }

    public RestClient build() {
      Objects.requireNonNull(baseUrl, "baseUrl is required");
      return new RestClient(this);
    }
  }
}
