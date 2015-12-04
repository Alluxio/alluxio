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

package tachyon.underfs.swift.direct.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import org.javaswift.joss.model.Access;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.underfs.swift.direct.SwiftDirectOutputStream;

public  class SwiftDirectClient {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public static SwiftDirectOutputStream PUT(Access access, String objectName) {
    LOG.debug("PUT method, object : {}", objectName);
    URL url;
    try {
      url = new URL(access.getPublicURL() + "/" + objectName);
      LOG.trace("Going to open URL connection");
      URLConnection connection = url.openConnection();
      LOG.trace("Connection open called");
      if (connection instanceof HttpURLConnection) {
        LOG.trace("Instance of HTTP URL Connection");
        HttpURLConnection httpCon = (HttpURLConnection) connection;
        LOG.trace("Created http url connection");
        httpCon.setRequestMethod("PUT");
        httpCon.addRequestProperty("X-Auth-Token", access.getToken());
        httpCon.addRequestProperty("Content-Type", "binary/octet-stream");
        httpCon.setDoInput(true);
        httpCon.setRequestProperty("Connection", "close");
        httpCon.setReadTimeout(100 * 1000);
        httpCon.setRequestProperty("Transfer-Encoding","chunked");
        httpCon.setDoOutput(true);
        httpCon.setChunkedStreamingMode(8 * 1024 * 1024);
        httpCon.connect();
        LOG.trace("About to create SwiftDirectOutputStream");
        SwiftDirectOutputStream outStream = new SwiftDirectOutputStream(
            httpCon);
        LOG.trace("SwiftDirectOutputStream created");
        return outStream;
      }
      LOG.debug("Not an instance of HTTP URL Connection");
      LOG.debug(connection.getClass().getName());
    } catch (MalformedURLException e) {
      LOG.error(e.getMessage());
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    return null;
  }
}
