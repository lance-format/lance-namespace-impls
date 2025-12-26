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
package org.lance.namespace.hive3;

import java.util.List;

/** Utility methods for pagination. */
public class PageUtil {

  private static final int DEFAULT_PAGE_SIZE = 100;

  private PageUtil() {}

  public static int normalizePageSize(Integer pageSize) {
    if (pageSize == null || pageSize <= 0) {
      return DEFAULT_PAGE_SIZE;
    }
    return pageSize;
  }

  public static Page splitPage(List<String> items, String pageToken, int pageSize) {
    int startIndex = 0;
    if (pageToken != null && !pageToken.isEmpty()) {
      try {
        startIndex = Integer.parseInt(pageToken);
      } catch (NumberFormatException e) {
        startIndex = 0;
      }
    }

    if (startIndex >= items.size()) {
      return new Page(java.util.Collections.emptyList(), null);
    }

    int endIndex = Math.min(startIndex + pageSize, items.size());
    List<String> pageItems = items.subList(startIndex, endIndex);

    String nextPageToken = endIndex < items.size() ? String.valueOf(endIndex) : null;
    return new Page(pageItems, nextPageToken);
  }

  public static class Page {
    private final List<String> items;
    private final String nextPageToken;

    public Page(List<String> items, String nextPageToken) {
      this.items = items;
      this.nextPageToken = nextPageToken;
    }

    public List<String> items() {
      return items;
    }

    public String nextPageToken() {
      return nextPageToken;
    }
  }
}
