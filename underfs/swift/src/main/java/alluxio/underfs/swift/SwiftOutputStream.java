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

package alluxio.underfs.swift;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for writing data to Swift API based object store.
 */
@NotThreadSafe
public class SwiftOutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(SwiftOutputStream.class);

  private OutputStream mOutputStream;
  private HttpURLConnection mHttpCon;

  /**
   * Creates a new instance of {@link SwiftOutputStream}.
   *
   * @param httpCon connection to Swift
   */
  public SwiftOutputStream(HttpURLConnection httpCon) throws IOException {
    try {
      mOutputStream  = httpCon.getOutputStream();
      mHttpCon = httpCon;
    } catch (Exception e) {
      LOG.error("Failed to construct SwiftOutputStream", e);
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
    InputStream is = null;
    try {
      // Status 400 and up should be read from error stream.
      // Expecting here 201 Create or 202 Accepted.
      if (mHttpCon.getResponseCode() >= 400) {
        LOG.error("Failed to write data to Swift with error code: " + mHttpCon.getResponseCode());
        is = mHttpCon.getErrorStream();
      } else {
        is = mHttpCon.getInputStream();
      }
      is.close();
    } catch (Exception e) {
      LOG.error("Failed to close SwiftOutputStream", e);
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
