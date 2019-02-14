/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.util;

import alluxio.AlluxioURI;
import alluxio.util.io.PathUtils;

import com.google.common.base.Joiner;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for working with URIs.
 */
@ThreadSafe
public final class URIUtils {
  public static final char QUERY_SEPARATOR = '&';
  public static final char QUERY_KEY_VALUE_SEPARATOR = '=';
  private static final int TO_LOWER_OFFSET = 'a' - 'A';

  private URIUtils() {} // prevent instantiation

  /**
   * Appends the given path to the given base URI.
   *
   * @param base the base URI
   * @param path the path to append
   * @return the URI resulting from appending the base and the path
   * @throws URISyntaxException if URI syntax error is encountered
   */
  public static URI appendPath(URI base, String path) throws URISyntaxException {
    return new URI(base.getScheme(), base.getAuthority(),
        PathUtils.concatPath(base.getPath(), path), base.getQuery(), base.getFragment());
  }

  /**
   * Appends the given path to the given base URI. It throws an {@link RuntimeException} if
   * the inputs are malformed.
   *
   * @param base the base URI
   * @param path the path to append
   * @return the URI resulting from appending the base and the path
   */
  public static URI appendPathOrDie(URI base, String path) {
    try {
      return appendPath(base, path);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generates a query string from a {@link Map <String, String>} of key/value pairs.
   *
   * @param queryMap the map of query key/value pairs
   * @return the generated query string, null if the input map is null or empty
   */
  @Nullable
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

  /**
   * @param uri a uri to check
   * @return whether the uri represents a local filesystem UFS path
   */
  public static boolean isLocalFilesystem(String uri) {
    return uri.startsWith(AlluxioURI.SEPARATOR)
        || uri.startsWith("file://")
        || AlluxioURI.hasWindowsDrive(uri, false);
  }

  /**
   * @param s1 first string (can be null)
   * @param s2 second string (can be null)
   * @return negative integer, zero, or positive integer, if the first string is less
   *         than, equal to, or greater than the second string
   */
  public static int compare(String s1, String s2) {
    if (s1 == null && s2 == null) {
      return 0;
    }
    if (s1 == null) {
      return -1;
    }
    if (s2 == null) {
      return 1;
    }
    return s1.compareTo(s2);
  }

  /**
   * Checks if two (nullable) strings are equal. Handles `%` codes in URIs.
   *
   * @param s1 the first string to compare (can be null)
   * @param s2 the second string to compare (can be null)
   * @return true if the strings are equal
   */
  public static boolean equals(String s1, String s2) {
    if (s1 == null && s2 == null) {
      return true;
    }
    if ((s1 != null) && (s2 != null)) {
      if (s1.length() != s2.length()) {
        return false;
      }
      if (s1.indexOf('%') < 0) {
        return s1.equals(s2);
      }
      int length = s1.length();
      for (int i = 0; i < length; i++) {
        char c1 = s1.charAt(i);
        char c2 = s2.charAt(i);
        if (c1 != '%') {
          if (c1 != c2) {
            return false;
          }
          continue;
        }
        // c1 is '%'
        if (c2 != '%') {
          return false;
        }
        // check next 2 characters
        i++;
        if (toLower(s1.charAt(i)) != toLower(s2.charAt(i))) {
          return false;
        }
        i++;
        if (toLower(s1.charAt(i)) != toLower(s2.charAt(i))) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * @param c the character to convert
   * @return the lowercase counterpart to the input character
   */
  public static int toLower(char c) {
    if (c >= 'A' && c <= 'Z') {
      return c + TO_LOWER_OFFSET;
    }
    return c;
  }

  /**
   * Returns true if the path requires normalization. A path needs normalization if it has segments
   * of: ".", "..", or consecutive "/".
   * @param path path to check
   * @return true if the path requires normalization
   */
  public static boolean needsNormalization(String path) {
    int last = path.length() - 1;
    int curr = 0;

    // Skip initial slashes
    while (curr <= last) {
      if (path.charAt(curr) != '/') {
        break;
      }
      curr++;
    }
    if (curr > 1) {
      return true;
    }

    while (curr <= last) {
      // segment is "." or "./"
      if ((path.charAt(curr) == '.') && ((curr == last) || (path.charAt(curr + 1) == '/'))) {
        return true;
      }
      // segment is ".." or "../"
      if ((path.charAt(curr) == '.') && (path.charAt(curr + 1) == '.') && ((curr + 1 == last) || (
          path.charAt(curr + 2) == '/'))) {
        return true;
      }
      while (curr <= last) {
        // Find next '/'
        if (path.charAt(curr++) != '/') {
          continue;
        }
        // Previous 'curr' was a '/'. Check for redundant slashes.
        if (path.charAt(curr) != '/') {
          break;
        }
        // multiple slashes
        return true;
      }
    }
    return false;
  }

  /**
   * Hashes a string for a URI hash. Handles octets.
   *
   * @param hash the input hash
   * @param s the string to hash and combine with the input hash
   * @return the resulting hash
   */
  public static int hash(int hash, String s) {
    if (s == null) {
      return hash;
    }
    // similar to Arrays.hashCode
    return s.indexOf('%') < 0 ? 31 * hash + s.hashCode() : normalizedHash(hash, s);
  }

  /**
   * Hashes a string for a URI hash, while ignoring the case.
   *
   * @param hash the input hash
   * @param s the string to hash and combine with the input hash
   * @return the resulting hash
   */
  public static int hashIgnoreCase(int hash, String s) {
    if (s == null) {
      return hash;
    }
    int length = s.length();
    for (int i = 0; i < length; i++) {
      hash = 31 * hash + URIUtils.toLower(s.charAt(i));
    }
    return hash;
  }

  /**
   * Hashes a string (which includes octets) for a URI hash.
   *
   * @param hash the input hash
   * @param s the string to hash and combine with the input hash
   * @return the resulting hash
   */
  private static int normalizedHash(int hash, String s) {
    int nextHash = 0;
    for (int index = 0; index < s.length(); index++) {
      char ch = s.charAt(index);
      nextHash = 31 * nextHash + ch;
      if (ch == '%') {
        // hash next 2 characters
        index++;
        nextHash = 31 * nextHash + URIUtils.toLower(s.charAt(index));
        index++;
        nextHash = 31 * nextHash + URIUtils.toLower(s.charAt(index));
      }
    }
    // similar to Arrays.hashCode
    return hash * 31 + nextHash;
  }
}
