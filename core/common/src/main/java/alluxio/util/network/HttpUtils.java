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

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Utility methods for working with http.
 */
public final class HttpUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

  private HttpUtils() {
  }

  /**
   * use post method to send params by http.
   * @param url the http url
   * @param timeout millisecond to wait for the server to respond before giving up
   * @return the response body stream
   */
  public static InputStream post(String url, Integer timeout) throws IOException {
    PostMethod postMethod = null;
    try {
      HttpClient httpClient = new HttpClient();
      if (null != timeout) {
        httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(timeout);
        httpClient.getHttpConnectionManager().getParams().setSoTimeout(timeout);
      }
      postMethod = new PostMethod(url);
      int statusCode = httpClient.executeMethod(postMethod);
      if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_CREATED) {
        return postMethod.getResponseBodyAsStream();
      } else {
        LOG.error("HTTP POST error code:" + statusCode);
      }
    } catch (Exception e) {
      LOG.error("HTTP POST error code:", e);
      throw e;
    } finally {
      if (postMethod != null) {
        postMethod.releaseConnection();
      }
    }
    return null;
  }
}
