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

package tachyon.underfs.swift;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;

/**
 * Swift output stream implements OutputStream.
 * This class is used to write data into Swift
 * Class is not thread-safe
 */
public class SwiftOutputStream extends OutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private OutputStream mOutputStream;
  private HttpURLConnection mHttpCon;

  /**
   * @param httpCon connection to Swift
   * @throws IOException on failure to get OutputStream
   */
  public SwiftOutputStream(HttpURLConnection httpCon) throws IOException {
    try {
      mOutputStream  = httpCon.getOutputStream();
      mHttpCon = httpCon;
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new IOException(e);
    }
  }

  @Override
  public void write(int b) throws IOException {
    mOutputStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mOutputStream.write(b, off, len);
  }

  @Override
  public void write(byte[] b) throws IOException {
    mOutputStream.write(b);
  }

  @Override
  public void close() throws IOException {
    mOutputStream.close();
    BufferedReader reader = null;
    InputStream is = null;
    try {
      // Status 400 and up should be read from error stream
      // Expecting here 201 Create or 202 Accepted
      if (mHttpCon.getResponseCode() >= 400) {
        is = mHttpCon.getErrorStream();
      } else {
        is = mHttpCon.getInputStream();
      }
      reader = new BufferedReader(new InputStreamReader(is));
      is.close();
      reader.close();
    } catch (Exception e) {
      LOG.error(e.getMessage());
      if (reader != null) {
        reader.close();
      }
      if (is != null) {
        is.close();
      }
    }
    mHttpCon.disconnect();
  }

  @Override
  public void flush() throws IOException {
    mOutputStream.flush();
  }
}
