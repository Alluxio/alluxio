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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A wrapped DO of the HTTP request URI.
 */
public class HttpRequestUri {

  private String mHost;

  private int mPort;

  private String mVersion;

  private String mMappingPath;

  private List<String> mRemainingFields;

  private Map<String, String> mParameters;

  private HttpRequestUri() {
  }

  /**
   * Create a {@link HttpRequestUri} object from a list of fields.
   * @param fields a list of fields that are built from the HTTP request URI
   * @return a {@link HttpRequestUri} object
   */
  public static HttpRequestUri of(List<String> fields) {
    HttpRequestUri httpRequestUri = new HttpRequestUri();

    String hostAndPort = fields.get(0);
    int index = hostAndPort.indexOf(":");
    if (index != -1) {
      String[] hostAndPortArray = hostAndPort.split(":");
      httpRequestUri.setHost(hostAndPortArray[0]);
      httpRequestUri.setPort(Integer.parseInt(hostAndPortArray[1]));
    } else {
      httpRequestUri.setHost(hostAndPort);
      httpRequestUri.setPort(80);
    }

    httpRequestUri.setVersion(fields.get(1));
    httpRequestUri.setMappingPath(fields.get(2));

    String lastField = fields.get(fields.size() - 1);
    if (lastField.startsWith("?")) {
      lastField = lastField.substring(1);
      String[] params = lastField.split("&");
      Map<String, String> parametersMap = new HashMap<>();
      for (String param : params) {
        String[] keyValue = param.split("=");
        parametersMap.put(keyValue[0], keyValue[1]);
      }
      httpRequestUri.setParameters(parametersMap);
      httpRequestUri.setRemainingFields(fields.subList(3, fields.size() - 1));
    } else {
      httpRequestUri.setRemainingFields(fields.subList(3, fields.size()));
    }
    return httpRequestUri;
  }

  /**
   * Get the host name of the request URI.
   * @return the host name of the request URI
   */
  public String getHost() {
    return mHost;
  }

  private void setHost(String host) {
    mHost = host;
  }

  /**
   * Get the port of the request URI.
   * @return the port of the request URI
   */
  public int getPort() {
    return mPort;
  }

  private void setPort(int port) {
    mPort = port;
  }

  /**
   * Get the version of the request URI.
   * @return the version of the request URI
   */
  public String getVersion() {
    return mVersion;
  }

  private void setVersion(String version) {
    mVersion = version;
  }

  /**
   * Get the mapping path of the request URI.
   * @return the mapping path of the request URI
   */
  public String getMappingPath() {
    return mMappingPath;
  }

  private void setMappingPath(String mappingPath) {
    mMappingPath = mappingPath;
  }

  /**
   * Get the remaining fields of the request URI.
   * @return the remaining fields of the request URI
   */
  public List<String> getRemainingFields() {
    return mRemainingFields;
  }

  private void setRemainingFields(List<String> remainingFields) {
    mRemainingFields = remainingFields;
  }

  /**
   * Get the parameters map.
   * @return the parameters map
   */
  public Map<String, String> getParameters() {
    return mParameters;
  }

  private void setParameters(Map<String, String> parameters) {
    mParameters = parameters;
  }
}
