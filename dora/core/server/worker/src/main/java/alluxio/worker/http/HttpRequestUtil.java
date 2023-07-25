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

package alluxio.worker.http;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Util class for handling HTTP request.
 */
public final class HttpRequestUtil {

  /**
   * Parse the parameters from the HTTP request URI and return a map of the parameters.
   *
   * @param requestUri the HTTP request URI
   * @return a map of the parameters
   */
  public static Map<String, String> parseRequestParameters(String requestUri) {
    requestUri = requestUri.substring(requestUri.indexOf("?") + 1);
    String[] params = requestUri.split("&");
    Map<String, String> parametersMap = new HashMap<>();
    for (String param : params) {
      String[] keyValue = param.split("=");
      parametersMap.put(keyValue[0], keyValue[1]);
    }
    return parametersMap;
  }

  /**
   * Parse the HTTP request URI and extract the fields.
   *
   * @param requestUri the HTTP request URI
   * @return a list of fields
   */
  public static List<String> extractFieldsFromHttpRequestUri(String requestUri) {
    int paramIndex = requestUri.indexOf("?");
    Optional<String> parameterString = Optional.empty();
    if (paramIndex != -1) {
      parameterString = Optional.of(requestUri.substring(paramIndex));
      requestUri = requestUri.substring(0, paramIndex);
    }

    List<String> fields = new ArrayList<>();
    String httpPrefix = "http://";
    String httpsPrefix = "https://";
    int leftIndex = 0;
    if (requestUri.startsWith(httpPrefix)) {
      leftIndex = httpPrefix.length();
    } else if (requestUri.startsWith(httpsPrefix)) {
      leftIndex = httpsPrefix.length();
    }
    // extract host and port
    int rightIndex = requestUri.indexOf("/", leftIndex);
    String hostAndPort = requestUri.substring(leftIndex, rightIndex);
    fields.add(hostAndPort);

    // begin to extract the fields
    leftIndex = rightIndex + 1;
    rightIndex = requestUri.indexOf("/", leftIndex);
    while (rightIndex != -1) {
      String field = requestUri.substring(leftIndex, rightIndex);
      fields.add(field);
      leftIndex = rightIndex + 1;
      rightIndex = requestUri.indexOf("/", leftIndex);
    }

    if (leftIndex >= requestUri.length()) {
      // no need to continue extracting since the last field has been extracted
      return fields;
    }

    // extract the last field
    String lastField = requestUri.substring(leftIndex);
    fields.add(lastField);

    if (parameterString.isPresent()) {
      // add parameters string as there are parameters
      fields.add(parameterString.get());
    }

    return fields;
  }
}
