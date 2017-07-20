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
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Utility methods for working with http.
 */
public final class HttpUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

  private HttpUtils() {
  }

  /**
   * Uses the post method to send a url with arguments by http, this method can call RESTful Api.
   *
   * @param url the http url
   * @param timeout milliseconds to wait for the server to respond before giving up
   * @param processInputStream the response body stream processor
   * @return the response body stream if response status is OK or CREATED, otherwise return null
   */
  public static String post(String url, Integer timeout,
                                 IProcessInputStream processInputStream)
      throws IOException {
    Preconditions.checkNotNull(timeout, "timeout");
    PostMethod postMethod = new PostMethod(url);
    try {
      HttpClient httpClient = new HttpClient();
      httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(timeout);
      httpClient.getHttpConnectionManager().getParams().setSoTimeout(timeout);
      int statusCode = httpClient.executeMethod(postMethod);
      if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_CREATED) {
        InputStream inputStream = postMethod.getResponseBodyAsStream();
        if (processInputStream != null) {
          processInputStream.process(inputStream);
          return null;
        } else {
          StringBuilder contentBuffer = new StringBuilder();
          try (BufferedReader br = new BufferedReader(
              new InputStreamReader(inputStream, "UTF-8"))) {
            String line;
            while ((line = br.readLine()) != null) {
              contentBuffer.append(line);
            }
          }
          return contentBuffer.toString();
        }
      } else {
        throw new IOException("Failed to perform POST request. Status code: " + statusCode);
      }
    } finally {
      postMethod.releaseConnection();
    }
  }

  public interface IProcessInputStream{
    /**
     * @param inputStream the input stream to process
     */
    void process(InputStream inputStream) throws IOException;
  }
}
