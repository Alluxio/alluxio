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

import alluxio.exception.status.InvalidArgumentException;
import alluxio.s3.S3Error;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.base.Joiner;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.Response;

/**
 * Represents a REST API test case.
 */
@NotThreadSafe
public final class TestCase {
  // make sure that serialization of empty objects does not fail
  private static final ObjectMapper XML_MAPPER = new XmlMapper()
      .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
      .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

  private final String mHostname;
  private final int mPort;
  private final String mBaseUri;
  private final String mEndpoint;
  private final Map<String, String> mParameters;
  private final String mMethod;
  private final TestCaseOptions mOptions;
  private final HttpURLConnection mConnection;

  /**
   * Creates a new instance of {@link TestCase}.
   *
   * @param hostname   the hostname to use
   * @param port       the port to use
   * @param baseUri    the base URI of the REST server
   * @param endpoint   the endpoint to use
   * @param parameters the parameters to use
   * @param method     the method to use
   * @param options    the test case options to use
   */
  public TestCase(String hostname, int port, String baseUri, String endpoint,
                  Map<String, String> parameters, String method, TestCaseOptions options)
      throws Exception {
    // TODO(czhu): URL-encode the URI & parameters
    mHostname = hostname;
    mPort = port;
    mBaseUri = baseUri;
    mEndpoint = endpoint;
    mParameters = parameters;
    mMethod = method;
    mOptions = options;
    mConnection =  execute();
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
    String url = String.format("http://%s:%s%s/%s", mHostname, mPort, mBaseUri, mEndpoint);
    if (mParameters.size() == 0) {
      return new URL(url);
    }
    Map<Boolean, List<Map.Entry<String, String>>> streams = mParameters.entrySet().stream().collect(
        Collectors.partitioningBy(e -> e.getValue().isEmpty()));
    Joiner j = Joiner.on("&");
    String emptyValParams = j.join(streams.get(true).stream().map(Map.Entry::getKey).collect(
        Collectors.toList()));
    String argParams = j.withKeyValueSeparator("=").join(streams.get(false).stream().collect(
        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    url = String.format("%s?%s&%s", url, emptyValParams, argParams);
    return new URL(url);
  }

  /**
   * @return the String from the InputStream of HttpURLConnection
   */
  public String getResponse() throws Exception {
    StringBuilder sb = new StringBuilder();
    BufferedReader br = new BufferedReader(new InputStreamReader(mConnection.getInputStream()));
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
    for (Map.Entry<String, String> entry : mOptions.getHeaders().entrySet()) {
      connection.setRequestProperty(entry.getKey(), entry.getValue());
    }
    if (mOptions.getBody() != null) {
      connection.setDoOutput(true);
      switch (mOptions.getContentType()) {
        case TestCaseOptions.XML_CONTENT_TYPE: // encode as XML string
          try (OutputStream os = connection.getOutputStream()) {
            os.write(XML_MAPPER.writeValueAsBytes(mOptions.getBody()));
          }
          break;
        case TestCaseOptions.JSON_CONTENT_TYPE: // encode as JSON string
          try (OutputStream os = connection.getOutputStream()) {
            os.write(JSON_MAPPER.writeValueAsBytes(mOptions.getBody()));
          }
          break;
        case TestCaseOptions.OCTET_STREAM_CONTENT_TYPE: // encode as-is
          try (OutputStream os = connection.getOutputStream()) {
            os.write((byte[]) mOptions.getBody());
          }
          break;
        case TestCaseOptions.TEXT_PLAIN_CONTENT_TYPE: // encode string using the charset
          try (OutputStream os = connection.getOutputStream()) {
            os.write(((String) mOptions.getBody()).getBytes(mOptions.getCharset()));
          }
          break;
        default:
          throw new InvalidArgumentException(String.format(
              "No mapper available for content type %s in TestCaseOptions!",
              mOptions.getContentType()));
      }
    }

    connection.connect();
    return connection;
  }

  /**
   * if response is not successful, it will assert fail.
   * @return connection
   * @throws Exception
   * @deprecated use checkResponseCode() instead.
   */
  @Deprecated
  public HttpURLConnection executeAndAssertSuccess() throws Exception {
    HttpURLConnection connection = execute();
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
   * @deprecated use checkResponse() instead.
   */
  @Deprecated
  public String runAndGetResponse() throws Exception {
    executeAndAssertSuccess();
    return getResponse();
  }

  /**
   * @deprecated use checkResponseCode() instead.
   */
  @Deprecated
  public void runAndCheckResult() throws Exception {
    runAndCheckResult(null);
  }

  /**
   * Runs the test case.
   * @deprecated use checkResponse() instead.
   */
  @Deprecated
  public void runAndCheckResult(Object expectedResult) throws Exception {
    String expected = "";
    if (expectedResult != null) {
      expected = convertToString(expectedResult);
    }

    String result = runAndGetResponse();
    System.out.println(result);
    Assert.assertEquals(mEndpoint, expected, result);
  }

  public TestCase checkResponse(@NotNull Object expectedResult) throws Exception {
    String expected = convertToString(expectedResult);
    Assert.assertEquals(mEndpoint, expected, getResponse());
    return this;
  }

  public TestCase checkResponseCode(@NotNull int expected) throws Exception {
    Assert.assertEquals(mEndpoint, expected, mConnection.getResponseCode());
    return this;
  }

  public TestCase checkHeader(@NotNull String header, @NotNull String expected) throws Exception {
    Assert.assertEquals(mEndpoint, expected, mConnection.getHeaderField(header));
    return this;
  }

  public TestCase checkErrorCode(@NotNull String expectedErrorCode) throws Exception {
    InputStream errorStream = mConnection.getErrorStream();
    S3Error response = XML_MAPPER.readerFor(S3Error.class).readValue(errorStream);
    Assert.assertEquals(mEndpoint, expectedErrorCode, response.getCode());
    return this;
  }

  private String convertToString(@NotNull Object expectedResult) throws Exception {
    switch (mOptions.getContentType()) {
      case TestCaseOptions.JSON_CONTENT_TYPE: {
        ObjectMapper mapper = new ObjectMapper();
        if (mOptions.isPrettyPrint()) {
          return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(expectedResult);
        } else {
          return mapper.writeValueAsString(expectedResult);
        }
      }
      case TestCaseOptions.XML_CONTENT_TYPE: {
        XmlMapper mapper = new XmlMapper();
        if (mOptions.isPrettyPrint()) {
          return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(expectedResult);
        } else {
          return mapper.writeValueAsString(expectedResult);
        }
      }
      case TestCaseOptions.OCTET_STREAM_CONTENT_TYPE: {
        return new String((byte[]) expectedResult, mOptions.getCharset());
      }
      case TestCaseOptions.TEXT_PLAIN_CONTENT_TYPE: {
        return (String) expectedResult;
      }
      default:
        throw new InvalidArgumentException("Invalid content type in TestCaseOptions!");
    }
  }
}
