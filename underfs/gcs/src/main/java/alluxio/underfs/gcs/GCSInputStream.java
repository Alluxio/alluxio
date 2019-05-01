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

import org.apache.commons.httpclient.HttpStatus;
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

  /** The storage object that will be updated on each large skip. */
  private GSObject mObject;

  /** The underlying input stream. */
  private BufferedInputStream mInputStream;

  /** Position of the stream. */
  private long mPos;

  /**
   * Creates a new instance of {@link GCSInputStream}.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the client for GCS
   * @param retryPolicy retry policy in case the key does not exist
   */
  GCSInputStream(String bucketName, String key, GoogleStorageService client,
      RetryPolicy retryPolicy) throws ServiceException {
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
      long pos, RetryPolicy retryPolicy) throws ServiceException {
    mBucketName = bucketName;
    mKey = key;
    mClient = client;
    mPos = pos;
    mObject = getObjectWithRetry(retryPolicy);
    mInputStream = new BufferedInputStream(mObject.getDataInputStream());
  }

  /**
   * Retries getting a {@link GSObject}.
   *
   * @param retryPolicy the retry policy to solve eventual consistency issue
   * @return the {@link GSObject}
   */
  private GSObject getObjectWithRetry(RetryPolicy retryPolicy) throws ServiceException {
    ServiceException lastException = null;
    while (retryPolicy.attempt()) {
      try {
        if (mPos > 0) {
          return mClient.getObject(mBucketName, mKey, null, null, null, null, mPos, null);
        } else {
          return mClient.getObject(mBucketName, mKey);
        }
      } catch (ServiceException e) {
        LOG.warn("Attempt {} to open key {} in bucket {} failed with exception : {}",
            retryPolicy.getAttemptCount(), mKey, mBucketName, e.toString());
        if (e.getResponseCode() != HttpStatus.SC_NOT_FOUND) {
          throw e;
        }
        // Key does not exist
        lastException = e;
      }
    }
    // Failed after retrying key does not exist
    throw lastException;
  }

  @Override
  public void close() throws IOException {
    mInputStream.close();
  }

  @Override
  public int read() throws IOException {
    int ret = mInputStream.read();
    if (ret != -1) {
      mPos++;
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
   * This method leverages the ability to open a stream from GCS from a given offset. When the
   * underlying stream has fewer bytes buffered than the skip request, the stream is closed, and
   * a new stream is opened starting at the requested offset.
   *
   * @param n number of bytes to skip
   * @return the number of bytes skipped
   */
  @Override
  public long skip(long n) throws IOException {
    if (mInputStream.available() >= n) {
      return mInputStream.skip(n);
    }
    // The number of bytes to skip is possibly large, open a new stream from GCS.
    mInputStream.close();
    mPos += n;
    try {
      mObject = mClient.getObject(mBucketName, mKey, null /* ignore ModifiedSince */,
          null /* ignore UnmodifiedSince */, null /* ignore MatchTags */,
          null /* ignore NoneMatchTags */, mPos /* byteRangeStart */,
          null /* ignore byteRangeEnd */);
      mInputStream = new BufferedInputStream(mObject.getDataInputStream());
    } catch (ServiceException e) {
      throw new IOException(e);
    }
    return n;
  }
}
