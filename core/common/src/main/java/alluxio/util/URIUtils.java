/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.util;

import alluxio.Constants;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for working with {@link alluxio.AlluxioURI}.
 */
@ThreadSafe
public final class URIUtils {
  private URIUtils() {} // prevent instantiation

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  public static final char QUERY_SEPARATOR = '&';
  public static final char QUERY_KEY_VALUE_SEPARATOR = '=';

  /**
   * Generates a query string from a {@link Map <String, String>} of key/value pairs.
   *
   * @param queryMap the map of query key/value pairs
   * @return the generated query string, null if the input map is null or empty
   */
  public static String generateQueryString(Map<String, String> queryMap) {
    if (queryMap == null || queryMap.isEmpty()) {
      return null;
    }
    ArrayList<String> pairs = new ArrayList<>(queryMap.size());
    try {
      for (Map.Entry<String, String> entry : queryMap.entrySet()) {
        pairs.add(
            URLEncoder.encode(entry.getKey(), "UTF-8") + QUERY_KEY_VALUE_SEPARATOR + URLEncoder
                .encode(entry.getValue(), "UTF-8"));
      }
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }

    Joiner joiner = Joiner.on(QUERY_SEPARATOR);
    return joiner.join(pairs);
  }

  /**
   * Parses the given query string, and returns a map of the query parameters.
   *
   * @param query the query string to parse
   * @return the map of query keys and values
   */
  public static Map<String, String> parseQueryString(String query) {
    Map<String, String> queryMap = new HashMap<>();
    if (query == null || query.isEmpty()) {
      return queryMap;
    }
    // The query string should escape '&'.
    String[] entries = query.split(String.valueOf(QUERY_SEPARATOR));

    try {
      for (String entry : entries) {
        // There should be at most 2 parts, since key and value both should escape '='.
        String[] parts = entry.split(String.valueOf(QUERY_KEY_VALUE_SEPARATOR));
        if (parts.length == 0) {
          // Skip this empty entry.
        } else if (parts.length == 1) {
          // There is no value part. Just use empty string as the value.
          String key = URLDecoder.decode(parts[0], "UTF-8");
          queryMap.put(key, "");
        } else {
          // Save the key and value.
          String key = URLDecoder.decode(parts[0], "UTF-8");
          String value = URLDecoder.decode(parts[1], "UTF-8");
          queryMap.put(key, value);
        }
      }
    } catch (UnsupportedEncodingException e) {
      // This is unexpected.
      throw new RuntimeException(e);
    }
    return queryMap;
  }
}
