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

package alluxio.rest;

import alluxio.Constants;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

import javax.ws.rs.core.Response;

/**
 * Represents a REST API test case.
 */
public final class TestCase {
  private String mEndpoint;
  private Map<String, String> mParameters;
  private String mMethod;
  private Object mExpectedResult;
  private String mHostname;
  private int mPort;
  private String mJsonString;
  private boolean mPrettyPrint;

  /**
   * Creates a new instance of {@link TestCase}.
   *
   * @param hostname the hostname to use
   * @param port the port to use
   * @param endpoint the endpoint to use
   * @param parameters the parameters to use
   * @param method the method to use
   * @param expectedResult the expected result to use
   */
  public TestCase(String hostname, int port, String endpoint, Map<String, String> parameters,
      String method, Object expectedResult) {
    this(hostname, port, endpoint, parameters, method, expectedResult, null, false);
  }

  /**
   * Creates a new instance of {@link TestCase} with JSON data.
   *
   * @param hostname the hostname to use
   * @param port the port to use
   * @param endpoint the endpoint to use
   * @param parameters the parameters to use
   * @param method the method to use
   * @param expectedResult the expected result to use
   * @param jsonString the json payload in string
   * @param prettyPrint if pretty prints the JSON response
   */
  public TestCase(String hostname, int port, String endpoint, Map<String, String> parameters,
      String method, Object expectedResult, String jsonString, boolean prettyPrint) {
    mHostname = hostname;
    mPort = port;
    mEndpoint = endpoint;
    mParameters = parameters;
    mMethod = method;
    mExpectedResult = expectedResult;
    mJsonString = jsonString;
    mPrettyPrint = prettyPrint;
  }

  /**
   * @return the endpoint
   */
  public String getEndpoint() {
    return mEndpoint;
  }

  /**
   * @return the method
   */
  public String getMethod() {
    return mMethod;
  }

  public URL createURL() throws Exception {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> parameter : mParameters.entrySet()) {
      sb.append(parameter.getKey() + "=" + parameter.getValue() + "&");
    }
    return new URL(
        "http://" + mHostname + ":" + mPort + "/" + Constants.REST_API_PREFIX + "/" + mEndpoint
            + "?" + sb.toString());
  }

  public String getResponse(HttpURLConnection connection) throws Exception {
    StringBuilder sb = new StringBuilder();
    BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
    char[] buffer = new char[1024];
    int len;

    while ((len = br.read(buffer)) > 0) {
      sb.append(buffer, 0, len);

    }
    br.close();

    return sb.toString();
  }

  /**
   * Runs the test case.
   *
   * @throws Exception if an error occurs
   */
  public void run() throws Exception {
    HttpURLConnection connection = (HttpURLConnection) createURL().openConnection();
    connection.setRequestMethod(mMethod);
    if (mJsonString != null) {
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
      OutputStream os = connection.getOutputStream();
      os.write(mJsonString.getBytes("UTF-8"));
      os.close();
    }

    connection.connect();
    Assert.assertEquals(mEndpoint, Response.Status.OK.getStatusCode(),
        connection.getResponseCode());
    String expected = "";
    if (mExpectedResult != null) {
      ObjectMapper mapper = new ObjectMapper();
      expected =
          mPrettyPrint ? mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mExpectedResult)
              : mapper.writeValueAsString(mExpectedResult);
    }
    Assert.assertEquals(mEndpoint, expected, getResponse(connection));
  }
}
