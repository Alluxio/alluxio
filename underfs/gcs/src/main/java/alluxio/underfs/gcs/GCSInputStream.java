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

package alluxio.underfs.gcs;

import alluxio.retry.RetryPolicy;

import org.apache.http.HttpStatus;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.GoogleStorageService;
import org.jets3t.service.model.GSObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for reading a file from GCS. The main purpose is to provide a faster skip method, as
 * the underlying implementation will read and discard bytes until the number to skip has been
 * reached. This input stream returns 0 when calling read with an empty buffer.
 */
@NotThreadSafe
public final class GCSInputStream extends InputStream {
  private static final Logger LOG = LoggerFactory.getLogger(GCSInputStream.class);

  /** Bucket name of the Alluxio GCS bucket. */
  private final String mBucketName;

  /** Key of the file in GCS to read. */
  private final String mKey;

  /** The JetS3t client for GCS operations. */
  private final GoogleStorageService mClient;

  /** The underlying input stream. */
  private BufferedInputStream mInputStream;

  /** Position of the stream. */
  private long mPos;

  /**
   * Policy determining the retry behavior in case the key does not exist. The key may not exist
   * because of eventual consistency.
   */
  private final RetryPolicy mRetryPolicy;

  /**
   * Creates a new instance of {@link GCSInputStream}.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the client for GCS
   * @param retryPolicy retry policy in case the key does not exist
   */
  GCSInputStream(String bucketName, String key, GoogleStorageService client,
      RetryPolicy retryPolicy) {
    this(bucketName, key, client, 0L, retryPolicy);
  }

  /**
   * Creates a new instance of {@link GCSInputStream}, at a specific position.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the client for GCS
   * @param pos the position to start
   * @param retryPolicy retry policy in case the key does not exist
   */
  GCSInputStream(String bucketName, String key, GoogleStorageService client,
      long pos, RetryPolicy retryPolicy) {
    mBucketName = bucketName;
    mKey = key;
    mClient = client;
    mPos = pos;
    mRetryPolicy = retryPolicy;
  }

  @Override
  public void close() throws IOException {
    closeStream();
  }

  @Override
  public int read() throws IOException {
    if (mInputStream == null) {
      openStream();
    }
    int value = mInputStream.read();
    if (value != -1) {
      mPos++;
    }
    return value;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    if (mInputStream == null) {
      openStream();
    }
    int ret = mInputStream.read(b, off, len);
    if (ret != -1) {
      mPos += ret;
    }
    return ret;
  }

  /**
   * This method leverages the ability to open a stream from GCS from a given offset. When the
   * underlying stream has fewer bytes buffered than the skip request, the stream is closed, and
   * a new stream is opened starting at the requested offset.
   *
   * @param n number of bytes to skip
   * @return the number of bytes skipped
   */
  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    if (mInputStream.available() >= n) {
      return mInputStream.skip(n);
    }
    closeStream();
    mPos += n;
    openStream();
    return n;
  }

  /**
   * Opens a new stream at mPos if the wrapped stream mInputStream is null.
   */
  private void openStream() throws IOException {
    ServiceException lastException = null;
    String errorMessage = String.format("Failed to open key: %s bucket: %s", mKey, mBucketName);
    while (mRetryPolicy.attempt()) {
      try {
        GSObject object;
        if (mPos > 0) {
          object = mClient.getObject(mBucketName, mKey, null, null, null, null, mPos, null);
        } else {
          object = mClient.getObject(mBucketName, mKey);
        }
        mInputStream = new BufferedInputStream(object.getDataInputStream());
        return;
      } catch (ServiceException e) {
        errorMessage = String
            .format("Failed to open key: %s bucket: %s attempts: %d error: %s", mKey, mBucketName,
                mRetryPolicy.getAttemptCount(), e.getMessage());
        if (e.getResponseCode() != HttpStatus.SC_NOT_FOUND) {
          throw new IOException(errorMessage, e);
        }
        // Key does not exist
        lastException = e;
      }
    }
    // Failed after retrying key does not exist
    throw new IOException(errorMessage, lastException);
  }

  /**
   * Closes the current stream.
   */
  private void closeStream() throws IOException {
    if (mInputStream == null) {
      return;
    }
    mInputStream.close();
    mInputStream = null;
  }
}
