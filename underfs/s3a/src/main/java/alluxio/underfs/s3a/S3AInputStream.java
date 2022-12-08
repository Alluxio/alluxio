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

package alluxio.underfs.s3a;

import alluxio.retry.RetryPolicy;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A wrapper around an {@link S3ObjectInputStream} which handles skips efficiently.
 */
@NotThreadSafe
public class S3AInputStream extends InputStream {
  /** Client for operations with s3. */
  protected AmazonS3 mClient;
  /** Name of the bucket the object resides in. */
  protected final String mBucketName;
  /** The path of the object to read. */
  protected final String mKey;
  protected final byte[] mSingleByteHolder = new byte[1];
  protected final GetObjectRequest mReadRequest;

  /** The backing input stream from s3. */
  protected S3ObjectInputStream mIn;
  /** The current position of the stream. */
  protected long mPos;

  /**
   * Policy determining the retry behavior in case the key does not exist. The key may not exist
   * because of eventual consistency.
   */
  protected final RetryPolicy mRetryPolicy;

  /**
   * Constructor for an input stream of an object in s3 using the aws-sdk implementation to read
   * the data. The stream will be positioned at the start of the file.
   *
   * @param bucketName the bucket the object resides in
   * @param key the path of the object to read
   * @param client the s3 client to use for operations
   * @param retryPolicy retry policy in case the key does not exist
   */
  public S3AInputStream(String bucketName, String key, AmazonS3 client, RetryPolicy retryPolicy) {
    this(bucketName, key, client, 0L, retryPolicy);
  }

  /**
   * Constructor for an input stream of an object in s3 using the aws-sdk implementation to read the
   * data. The stream will be positioned at the specified position.
   *
   * @param bucketName the bucket the object resides in
   * @param key the path of the object to read
   * @param client the s3 client to use for operations
   * @param position the position to begin reading from
   * @param retryPolicy retry policy in case the key does not exist
   */
  public S3AInputStream(String bucketName, String key, AmazonS3 client,
      long position, RetryPolicy retryPolicy) {
    mBucketName = bucketName;
    mKey = key;
    mClient = client;
    mPos = position;
    mRetryPolicy = retryPolicy;
    mReadRequest = new GetObjectRequest(bucketName, key);
  }

  @Override
  public int read() throws IOException {
    if (read(mSingleByteHolder) == -1) {
      return -1;
    }
    return mSingleByteHolder[0];
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int offset, int length) throws IOException {
    S3Object object = getObject(mPos, mPos + length);
    if (object == null) {
      // range request cannot meet
      object = getObject(mPos);
    }
    if (object == null) {
      // mPos already pass file end
      return -1;
    }
    try (S3ObjectInputStream in = object.getObjectContent()) {
      int ret = in.read(b, offset, length);
      if (ret == -1) {
        return ret;
      }
      mPos += ret;
      return ret;
    }
  }

  private S3Object getObject(long start) {
    return getObject(start, Long.MAX_VALUE - 1);
  }

  private S3Object getObject(long start, long end) {
    // If the position is 0, setting range is redundant and causes an error if the file is 0 length
    if (start > 0 || end > 0) {
      // end is inclusive, so minus one here
      mReadRequest.setRange(start, end - 1);
    }
    AlluxioS3Exception lastException;
    do {
      try {
        return getClient().getObject(mReadRequest);
      } catch (AmazonS3Exception e) {
        String errorMessage = String
            .format("Failed to get object: %s bucket: %s attempts: %d error: %s", mKey, mBucketName,
                mRetryPolicy.getAttemptCount(), e.getMessage());
        lastException = AlluxioS3Exception.from(errorMessage, e);
        if (!lastException.isRetryable()) {
          throw lastException;
        }
      }
    } while (mRetryPolicy.attempt());
    throw lastException;
  }

  @Override
  public long skip(long n) {
    if (n <= 0) {
      return 0;
    }
    mPos += n;
    return n;
  }

  /**
   * @return the client
   */
  protected AmazonS3 getClient() {
    return mClient;
  }
}
