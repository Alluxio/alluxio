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

package alluxio.rest;

import alluxio.Constants;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.io.ByteStreams;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;
import javax.ws.rs.core.Response;

/**
 * Represents a REST API test case.
 */
@NotThreadSafe
public final class TestCase {
  private String mHostname;
  private int mPort;
  private String mEndpoint;
  private Map<String, String> mParameters;
  private String mMethod;
  private Object mExpectedResult;
  private TestCaseOptions mOptions;

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
    this(hostname, port, endpoint, parameters, method, expectedResult, TestCaseOptions.defaults());
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
   * @param options the test case options to use
   */
  public TestCase(String hostname, int port, String endpoint, Map<String, String> parameters,
      String method, Object expectedResult, TestCaseOptions options) {
    mHostname = hostname;
    mPort = port;
    mEndpoint = endpoint;
    mParameters = parameters;
    mMethod = method;
    mExpectedResult = expectedResult;
    mOptions = options;
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

  /**
   * @return The URL which is created
   */
  public URL createURL() throws Exception {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> parameter : mParameters.entrySet()) {
      sb.append(parameter.getKey() + "=" + parameter.getValue() + "&");
    }
    return new URL(
        "http://" + mHostname + ":" + mPort + Constants.REST_API_PREFIX + "/" + mEndpoint
            + "?" + sb.toString());
  }

  /**
   * @param connection the HttpURLConnection
   * @return the String from the InputStream of HttpURLConnection
   */
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
   * Runs the test case and returns the output.
   */
  public String call() throws Exception {
    HttpURLConnection connection = (HttpURLConnection) createURL().openConnection();
    connection.setRequestMethod(mMethod);
    if (mOptions.getInputStream() != null) {
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/octet-stream");
      ByteStreams.copy(mOptions.getInputStream(), connection.getOutputStream());
    }
    if (mOptions.getBody() != null) {
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/json");
      ObjectMapper mapper = new ObjectMapper();
      // make sure that serialization of empty objects does not fail
      mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
      OutputStream os = connection.getOutputStream();
      os.write(mapper.writeValueAsString(mOptions.getBody()).getBytes());
      os.close();
    }

    connection.connect();
    if (connection.getResponseCode() != Response.Status.OK.getStatusCode()) {
      InputStream errorStream = connection.getErrorStream();
      if (errorStream != null) {
        Assert.fail("Request failed: " + IOUtils.toString(errorStream));
      }
      Assert.fail("Request failed with status code " + connection.getResponseCode());
    }
    return getResponse(connection);
  }

  /**
   * Runs the test case.
   */
  public void run() throws Exception {
    String expected = "";
    if (mExpectedResult != null) {
      ObjectMapper mapper = new ObjectMapper();
      if (mOptions.isPrettyPrint()) {
        expected = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mExpectedResult);
      } else {
        expected = mapper.writeValueAsString(mExpectedResult);
      }
    }
    String result = call();
    Assert.assertEquals(mEndpoint, expected, result);
  }
}
