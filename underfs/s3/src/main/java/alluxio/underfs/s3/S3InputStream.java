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

package alluxio.underfs.s3;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;

/**
 * A stream for reading a file from S3. The main purpose is to provide a faster skip method, as
 * the underlying implementation will read and discard bytes until the number to skip has been
 * reached. This input stream returns 0 when calling read with an empty buffer.
 */
@NotThreadSafe
public class S3InputStream extends InputStream {

  /** Bucket name of the Alluxio S3 bucket. */
  private final String mBucketName;

  /** Key of the file in S3 to read. */
  private final String mKey;

  /** The JetS3t client for S3 operations. */
  private final S3Service mClient;

  /** The storage object that will be updated on each large skip. */
  private S3Object mObject;

  /** The underlying input stream. */
  private BufferedInputStream mInputStream;

  /** Position of the stream. */
  private long mPos;

  /**
   * Creates a new instance of {@link S3InputStream}.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the client for S3
   * @throws ServiceException if a service exception occurs
   */
  S3InputStream(String bucketName, String key, S3Service client) throws ServiceException {
    mBucketName = bucketName;
    mKey = key;
    mClient = client;
    mObject = mClient.getObject(mBucketName, mKey);
    mInputStream = new BufferedInputStream(mObject.getDataInputStream());
  }

  @Override
  public void close() throws IOException {
    mInputStream.close();
  }

  @Override
  public int read() throws IOException {
    int ret = mInputStream.read();
    if (ret != -1) {
      mPos ++;
    }
    return ret;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int ret = mInputStream.read(b, off, len);
    if (ret != -1) {
      mPos += ret;
    }
    return ret;
  }

  /**
   * This method leverages the ability to open a stream from S3 from a given offset. When the
   * underlying stream has fewer bytes buffered than the skip request, the stream is closed, and
   * a new stream is opened starting at the requested offset.
   *
   * @param n number of bytes to skip
   * @return the number of bytes skipped
   * @throws IOException if an error occurs when requesting from S3
   */
  @Override
  public long skip(long n) throws IOException {
    if (mInputStream.available() >= n) {
      return mInputStream.skip(n);
    }
    // The number of bytes to skip is possibly large, open a new stream from S3.
    mInputStream.close();
    mPos += n;
    try {
      mObject = mClient.getObject(mBucketName, mKey, null, null, null, null, mPos, null);
      mInputStream = new BufferedInputStream(mObject.getDataInputStream());
    } catch (ServiceException e) {
      throw new IOException(e);
    }
    return n;
  }
}
