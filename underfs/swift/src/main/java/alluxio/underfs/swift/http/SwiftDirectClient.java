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

package alluxio.underfs.swift.http;

import alluxio.Constants;
import alluxio.underfs.swift.SwiftOutputStream;

import org.javaswift.joss.model.Access;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Contains direct calls to OpenStack Swift. This is needed to bypass certain limitations in the
 * JOSS package.
 */
@ThreadSafe
public class SwiftDirectClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int HTTP_READ_TIMEOUT = 100 * 1000;
  private static final int HTTP_CHUNK_STREAMING = 8 * 1024 * 1024;

  /**
   * Swift HTTP PUT request.
   *
   * @param access JOSS access object
   * @param objectName name of the object to create
   * @return SwiftOutputStream that will be used to upload data to Swift
   */
  public static SwiftOutputStream put(Access access, String objectName) {
    LOG.debug("PUT method, object : {}", objectName);
    URL url;
    try {
      url = new URL(access.getPublicURL() + "/" + objectName);
      URLConnection connection = url.openConnection();
      if (connection instanceof HttpURLConnection) {
        HttpURLConnection httpCon = (HttpURLConnection) connection;
        httpCon.setRequestMethod("PUT");
        httpCon.addRequestProperty("X-Auth-Token", access.getToken());
        httpCon.addRequestProperty("Content-Type", "binary/octet-stream");
        httpCon.setDoInput(true);
        httpCon.setRequestProperty("Connection", "close");
        httpCon.setReadTimeout(HTTP_READ_TIMEOUT);
        httpCon.setRequestProperty("Transfer-Encoding","chunked");
        httpCon.setDoOutput(true);
        httpCon.setChunkedStreamingMode(HTTP_CHUNK_STREAMING);
        httpCon.connect();
        SwiftOutputStream outStream = new SwiftOutputStream(
            httpCon);
        return outStream;
      }
      LOG.debug("Not an instance of HTTP URL Connection");
    } catch (MalformedURLException e) {
      LOG.error(e.getMessage());
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    return null;
  }
}
