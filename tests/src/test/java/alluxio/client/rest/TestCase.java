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

package alluxio.client.rest;

import alluxio.Constants;
import alluxio.exception.status.InvalidArgumentException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Represents a REST API test case.
 */
@NotThreadSafe
public final class TestCase {
  /* Required */
  private final String mHostname;
  private final int mPort;
  private final String mEndpoint;
  private final Map<String, String> mParameters;
  private final String mMethod;
  private final Object mExpectedResult;
  /* Optional */
  private final TestCaseOptions mOptions;
  private final String mServicePrefix;

  public TestCase(String hostname, int port, String endpoint, Map<String, String> parameters,
      String method, Object expectedResult) {
    this(hostname, port, endpoint, parameters, method, expectedResult,
        TestCaseOptions.defaults(), "");
  }

  public TestCase(String hostname, int port, String endpoint, Map<String, String> parameters,
      String method, Object expectedResult, TestCaseOptions options) {
    this(hostname, port, endpoint, parameters, method, expectedResult,
        options, "");
  }

  public TestCase(String hostname, int port, String endpoint, Map<String, String> parameters,
                  String method, Object expectedResult, String servicePrefix) {
    this(hostname, port, endpoint, parameters, method, expectedResult,
        TestCaseOptions.defaults(), servicePrefix);
  }

  /**
   * Creates a new instance of {@link TestCase}.
   *
   * @param hostname the hostname to use
   * @param port the port to use
   * @param endpoint the endpoint to use
   * @param parameters the parameters to use
   * @param method the method to use
   * @param expectedResult the expected result to use
   * @param options the test case options to use
   * @param servicePrefix the prefix to prepend to the endpoint
   */
  public TestCase(String hostname, int port, String endpoint, Map<String, String> parameters,
      String method, Object expectedResult, TestCaseOptions options, String servicePrefix) {
    mHostname = hostname;
    mPort = port;
    mEndpoint = endpoint;
    mParameters = parameters;
    mMethod = method;
    mExpectedResult = expectedResult;
    mOptions = options;
    mServicePrefix = servicePrefix;
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
    boolean addAmpersands = false;
    for (Map.Entry<String, String> parameter : mParameters.entrySet()) {
      if (!addAmpersands) { addAmpersands = true; }
      else { sb.append("&"); }
      sb.append(parameter.getKey());
      if (parameter.getValue() != null && !parameter.getValue().isEmpty()) {
        sb.append("=").append(parameter.getValue());
      }
    }
    String url = String.format("http://%s:%s%s/%s/%s",
        mHostname, mPort, Constants.REST_API_PREFIX, mServicePrefix, mEndpoint);
    if (sb.length() > 0) {
      url = String.format("%s?%s", url, sb);
    }
    return new URL(url);
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
   * Runs the test case and returns the {@link HttpURLConnection}.
   */
  public HttpURLConnection execute() throws Exception {
    HttpURLConnection connection = (HttpURLConnection) createURL().openConnection();
    connection.setRequestMethod(mMethod);
    if (mOptions.getMD5() != null) {
      connection.setRequestProperty("Content-MD5", mOptions.getMD5());
    }
    if (mOptions.getAuthorization() != null) {
      connection.setRequestProperty("Authorization", mOptions.getAuthorization());
    }
    if (mOptions.getInputStream() != null) {
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", MediaType.APPLICATION_OCTET_STREAM);
      ByteStreams.copy(mOptions.getInputStream(), connection.getOutputStream());
    }
    if (mOptions.getBody() != null) {
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", mOptions.getContentType());
      ObjectMapper mapper = new ObjectMapper();
      // make sure that serialization of empty objects does not fail
      mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
      OutputStream os = connection.getOutputStream();
      os.write(mapper.writeValueAsString(mOptions.getBody()).getBytes());
      os.close();
    }

    connection.connect();
    if (Response.Status.Family.familyOf(connection.getResponseCode())
        != Response.Status.Family.SUCCESSFUL) {
      InputStream errorStream = connection.getErrorStream();
      if (errorStream != null) {
        Assert.fail("Request failed: " + IOUtils.toString(errorStream));
      }
      Assert.fail("Request failed with status code " + connection.getResponseCode());
    }
    return connection;
  }

  /**
   * Runs the test case and returns the output.
   */
  public String call() throws Exception {
    return getResponse(execute());
  }

  /**
   * Runs the test case.
   */
  public void run() throws Exception {
    String expected = "";
    if (mExpectedResult != null) {
      switch (mOptions.getContentType()) {
        case TestCaseOptions.JSON_CONTENT_TYPE: {
          ObjectMapper mapper = new ObjectMapper();
          if (mOptions.isPrettyPrint()) {
            expected = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mExpectedResult);
          } else {
            expected = mapper.writeValueAsString(mExpectedResult);
          }
          break;
        }
        case TestCaseOptions.XML_CONTENT_TYPE: {
          XmlMapper mapper = new XmlMapper();
          if (mOptions.isPrettyPrint()) {
            expected = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mExpectedResult);
          } else {
            expected = mapper.writeValueAsString(mExpectedResult);
          }
          break;
        }
        default:
          throw new InvalidArgumentException("Invalid content type in TestCaseOptions!");
      }
    }
    String result = call();
    Assert.assertEquals(mEndpoint, expected, result);
  }
}
