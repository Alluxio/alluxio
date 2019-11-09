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

package alluxio.util.network;

import com.google.common.base.Preconditions;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Utility methods for working with http.
 */
public final class HttpUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

  private HttpUtils() {}

  /**
   * Uses the post method to send a url with arguments by http, this method can call RESTful Api.
   *
   * @param url the http url
   * @param timeout milliseconds to wait for the server to respond before giving up
   * @param processInputStream the response body stream processor
   */
  public static void post(String url, Integer timeout, IProcessInputStream processInputStream)
      throws IOException {
    Preconditions.checkNotNull(timeout, "timeout");
    Preconditions.checkNotNull(processInputStream, "processInputStream");
    PostMethod postMethod = new PostMethod(url);
    try {
      HttpClient httpClient = new HttpClient();
      httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(timeout);
      httpClient.getHttpConnectionManager().getParams().setSoTimeout(timeout);
      int statusCode = httpClient.executeMethod(postMethod);
      if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_CREATED) {
        InputStream inputStream = postMethod.getResponseBodyAsStream();
        processInputStream.process(inputStream);
      } else {
        throw new IOException("Failed to perform POST request. Status code: " + statusCode);
      }
    } finally {
      postMethod.releaseConnection();
    }
  }

  /**
   * Uses the post method to send a url with arguments by http, this method can call RESTful Api.
   *
   * @param url the http url
   * @param timeout milliseconds to wait for the server to respond before giving up
   * @return the response body stream as UTF-8 string if response status is OK or CREATED
   */
  public static String post(String url, Integer timeout) throws IOException {
    final StringBuilder contentBuffer = new StringBuilder();
    post(url, timeout, inputStream -> {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))) {
        String line;
        while ((line = br.readLine()) != null) {
          contentBuffer.append(line);
        }
      }
    });
    return contentBuffer.toString();
  }

  /**
   * Uses the get method to send a url with arguments by http, this method can call RESTful Api.
   *
   * @param url the http url
   * @param timeout milliseconds to wait for the server to respond before giving up
   * @return the response body stream if response status is OK or CREATED
   */
  public static InputStream getInputStream(String url, Integer timeout) throws IOException {
    Preconditions.checkNotNull(url, "url");
    Preconditions.checkNotNull(timeout, "timeout");
    GetMethod getMethod = new GetMethod(url);
    HttpClient httpClient = new HttpClient();
    httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(timeout);
    httpClient.getHttpConnectionManager().getParams().setSoTimeout(timeout);
    int statusCode = httpClient.executeMethod(getMethod);
    if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_CREATED) {
      throw new IOException("Failed to perform GET request. Status code: " + statusCode);
    }
    InputStream inputStream = getMethod.getResponseBodyAsStream();
    return new BufferedInputStream(inputStream) {
      @Override
      public void close() throws IOException {
        getMethod.releaseConnection();
      }
    };
  }

  /**
   * Uses the get method to send a url with arguments by http, this method can call RESTful Api.
   *
   * @param url the http url
   * @param timeout milliseconds to wait for the server to respond before giving up
   * @param processInputStream the response body stream processor
   */
  public static void get(String url, Integer timeout, IProcessInputStream processInputStream)
      throws IOException {
    Preconditions.checkNotNull(url, "url");
    Preconditions.checkNotNull(timeout, "timeout");
    Preconditions.checkNotNull(processInputStream, "processInputStream");

    try (InputStream inputStream = getInputStream(url, timeout)) {
      processInputStream.process(inputStream);
    }
  }

  /**
   * Uses the get method to send a url with arguments by http, this method can call RESTful Api.
   *
   * @param url the http url
   * @param timeout milliseconds to wait for the server to respond before giving up
   * @return the response content string if response status is OK or CREATED
   */
  public static String get(String url, Integer timeout) throws IOException {
    Preconditions.checkNotNull(url, "url");
    Preconditions.checkNotNull(timeout, "timeout");
    final StringBuilder contentBuffer = new StringBuilder();
    get(url, timeout, inputStream -> {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))) {
        String line;
        while ((line = br.readLine()) != null) {
          contentBuffer.append(line);
        }
      }
    });
    return contentBuffer.toString();
  }

  /**
   * Uses the head method to send a url with arguments by http, this method can call RESTful Api.
   *
   * @param url the http url
   * @param timeout milliseconds to wait for the server to respond before giving up
   * @return the response headers
   */
  public static Header[] head(String url, Integer timeout) {
    Preconditions.checkNotNull(url, "url");
    Preconditions.checkNotNull(timeout, "timeout");
    HeadMethod headMethod = new HeadMethod(url);
    try {
      HttpClient httpClient = new HttpClient();
      httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(timeout);
      httpClient.getHttpConnectionManager().getParams().setSoTimeout(timeout);
      int statusCode = httpClient.executeMethod(headMethod);
      if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_CREATED) {
        return headMethod.getResponseHeaders();
      } else {
        LOG.error("Failed to perform HEAD request. Status code: {}", statusCode);
      }
    } catch (IOException e) {
      LOG.error("Failed to execute URL request: {}", url, e);
    } finally {
      headMethod.releaseConnection();
    }

    return null;
  }

  /**
   * This interface should be implemented by the http response body stream processor.
   */
  public interface IProcessInputStream {
    /**
     * @param inputStream the input stream to process
     */
    void process(InputStream inputStream) throws IOException;
  }
}
