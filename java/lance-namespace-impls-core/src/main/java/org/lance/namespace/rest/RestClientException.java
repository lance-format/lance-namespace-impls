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

/**
 * Exception thrown when a REST API call fails.
 *
 * <p>Contains the HTTP status code and response body for error diagnosis.
 */
public class RestClientException extends RuntimeException {
  private final int statusCode;
  private final String responseBody;

  public RestClientException(int statusCode, String responseBody) {
    super(String.format("HTTP %d: %s", statusCode, responseBody));
    this.statusCode = statusCode;
    this.responseBody = responseBody;
  }

  public RestClientException(int statusCode, String message, Throwable cause) {
    super(String.format("HTTP %d: %s", statusCode, message), cause);
    this.statusCode = statusCode;
    this.responseBody = message;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public String getResponseBody() {
    return responseBody;
  }

  public boolean isClientError() {
    return statusCode >= 400 && statusCode < 500;
  }

  public boolean isServerError() {
    return statusCode >= 500;
  }

  public boolean isNotFound() {
    return statusCode == 404;
  }

  public boolean isConflict() {
    return statusCode == 409;
  }

  public boolean isBadRequest() {
    return statusCode == 400;
  }

  public boolean isUnauthorized() {
    return statusCode == 401;
  }

  public boolean isForbidden() {
    return statusCode == 403;
  }
}
