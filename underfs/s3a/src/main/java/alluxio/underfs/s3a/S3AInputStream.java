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
import alluxio.underfs.SeekableUnderFileInputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A wrapper around an {@link S3ObjectInputStream} which handles skips efficiently.
 */
@NotThreadSafe
public class S3AInputStream extends SeekableUnderFileInputStream {
  /** Client for operations with s3. */
  protected AmazonS3 mClient;
  /** Name of the bucket the object resides in. */
  protected final String mBucketName;
  /** The path of the object to read. */
  protected final String mKey;
  /** The current position of the stream. */
  protected long mPos;

  /**
   * Policy determining the retry behavior in case the key does not exist. The key may not exist
   * because of eventual consistency.
   */
  protected final RetryPolicy mRetryPolicy;

  /**
   * Constructor for an input stream of an object in s3 using the aws-sdk implementation to read the
   * data. The stream will be positioned at the specified position.
   *
   * @param bucketName the bucket the object resides in
   * @param key the path of the object to read
   * @param client the s3 client to use for operations
   * @param position the position to begin reading from
   * @param retryPolicy retry policy in case the key does not exist
   * @return a S3A input stream
   */
  public static S3AInputStream create(String bucketName, String key, AmazonS3 client,
      long position, RetryPolicy retryPolicy) {
    S3Object object = getObject(bucketName, key, client, position, retryPolicy);
    S3ObjectInputStream stream = object.getObjectContent();
    return new S3AInputStream(stream, bucketName, key, client, position, retryPolicy);
  }

  private static S3Object getObject(String bucketName, String key, AmazonS3 client,
      long position, RetryPolicy retryPolicy) {
    GetObjectRequest getReq = new GetObjectRequest(bucketName, key);
    // If the position is 0, setting range is redundant and causes an error if the file is 0 length
    if (position > 0) {
      getReq.setRange(position);
    }
    AlluxioS3Exception lastException;
    do {
      try {
        return client.getObject(getReq);
      } catch (AmazonS3Exception e) {
        String errorMessage = String
            .format("Failed to get object: %s bucket: %s attempts: %d error: %s", key, bucketName,
                retryPolicy.getAttemptCount(), e.getMessage());
        lastException = AlluxioS3Exception.from(errorMessage, e);
        if (!lastException.isRetryable()) {
          throw lastException;
        }
      }
    } while (retryPolicy.attempt());
    throw lastException;
  }

  private S3AInputStream(S3ObjectInputStream stream, String bucketName, String key, AmazonS3 client,
      long position, RetryPolicy retryPolicy) {
    super(stream);
    mBucketName = bucketName;
    mKey = key;
    mClient = client;
    mPos = position;
    mRetryPolicy = retryPolicy;
  }

  @Override
  public int read() throws IOException {
    int value = in.read();
    if (value != -1) { // valid data read
      mPos++;
    }
    return value;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int offset, int length) throws IOException {
    if (length == 0) {
      return 0;
    }
    int read = in.read(b, offset, length);
    if (read != -1) {
      mPos += read;
    }
    return read;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    long skipped = in.skip(n);
    mPos += skipped;
    return skipped;
  }

  @Override
  public long getPos() {
    return mPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos == mPos) {
      return;
    }
    if (pos < mPos) {
      reopenStream(pos);
      return;
    }
    do {
      skip(pos - mPos);
    } while (pos > mPos && in.available() > 0);
    if (mPos != pos) {
      throw new IOException(String.format("Failed to seek to pos %s but at %s", pos, mPos));
    }
  }

  /**
   * Reopen the stream at new position.
   *
   * @param newPos the new position to reopen the stream to
   */
  private void reopenStream(long newPos) throws IOException {
    in.close();
    S3Object object = getObject(mBucketName, mKey, mClient, newPos, mRetryPolicy);
    in = object.getObjectContent();
    mPos = newPos;
  }
}
