/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.underfs.swift.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import javax.annotation.concurrent.ThreadSafe;

import org.javaswift.joss.model.Access;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.underfs.swift.SwiftOutputStream;

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
