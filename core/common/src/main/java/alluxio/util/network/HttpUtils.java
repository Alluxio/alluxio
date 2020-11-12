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
import com.google.common.net.HttpHeaders;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

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
   * @param cookies the cookies to use in the request
   * @param timeout milliseconds to wait for the server to respond before giving up
   * @param processInputStream the response body stream processor
   */
  public static void post(String url, String cookies, Integer timeout,
                          IProcessInputStream processInputStream) throws IOException {
    Preconditions.checkNotNull(timeout, "timeout");
    Preconditions.checkNotNull(processInputStream, "processInputStream");

    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectionRequestTimeout(timeout)
        .setConnectTimeout(timeout)
        .setSocketTimeout(timeout)
        .build();
    try (CloseableHttpClient client =
             HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build()) {
      HttpUriRequest req;
      if (cookies.isEmpty()) {
        req = RequestBuilder.post(url).build();
      } else {
        req = RequestBuilder.post(url).setHeader(HttpHeaders.COOKIE, cookies).build();
      }

      HttpResponse response = client.execute(req);
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_CREATED) {
        InputStream inputStream = response.getEntity().getContent();
        processInputStream.process(inputStream);
      } else {
        // Print the full content for debugging
        if (LOG.isDebugEnabled()) {
          String content = IOUtils.toString(response.getEntity().getContent(),
                  StandardCharsets.ISO_8859_1);
          LOG.debug("Full content: {}", content);
        }
        throw new IOException(String.format("Failed to perform POST request: %s",
                response.getStatusLine()));
      }
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
    post(url, "", timeout, inputStream -> {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream,
              StandardCharsets.UTF_8))) {
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
    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectionRequestTimeout(timeout)
        .setConnectTimeout(timeout)
        .setSocketTimeout(timeout)
        .build();
    CloseableHttpClient client =
        HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();

    HttpResponse response = client.execute(RequestBuilder.get(url).build());
    int statusCode = response.getStatusLine().getStatusCode();

    if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_CREATED) {
      throw new IOException("Failed to perform GET request. Status code: " + statusCode);
    }
    InputStream inputStream = response.getEntity().getContent();
    return new BufferedInputStream(inputStream) {
      @Override
      public void close() throws IOException {
        client.close();
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

    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectionRequestTimeout(timeout)
        .setConnectTimeout(timeout)
        .setSocketTimeout(timeout)
        .build();
    try (CloseableHttpClient client =
             HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build()) {
      HttpResponse response = client.execute(RequestBuilder.head(url).build());
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_CREATED) {
        return response.getAllHeaders();
      } else {
        LOG.error("Failed to perform HEAD request. Status code: {}", statusCode);
      }
    } catch (Exception e) {
      LOG.error("Failed to execute URL request: {}", url, e);
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
