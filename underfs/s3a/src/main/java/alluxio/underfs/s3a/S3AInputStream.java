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
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A wrapper around an {@link S3ObjectInputStream} which handles skips efficiently.
 */
@NotThreadSafe
public class S3AInputStream extends InputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3AInputStream.class);

  /** Client for operations with s3. */
  protected AmazonS3 mClient;
  /** Name of the bucket the object resides in. */
  protected final String mBucketName;
  /** The path of the object to read. */
  protected final String mKey;

  /** The backing input stream from s3. */
  protected S3ObjectInputStream mIn;
  /** The current position of the stream. */
  protected long mPos;
  /** The current position of the S3 http source stream. */
  private long mInPos;

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
  }

  @Override
  public void close() {
    closeStream();
  }

  @Override
  public int read() throws IOException {
    seekStream(1);
    int value = mIn.read();
    if (value != -1) { // valid data read
      mPos++;
      mInPos++;
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
    seekStream(length);
    int read = mIn.read(b, offset, length);
    if (read != -1) {
      mPos += read;
      mInPos += read;
    }
    return read;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    mPos += n;
    return n;
  }

  /**
   * Seek if we could reuse the existing stream or create a new one.
   * @param length the content length to be read
   */
  private void seekStream(int length) throws IOException {
    if (mIn != null && mPos == mInPos) { // stream is already open and at the expected position
      return;
    }
    if (mIn != null && mPos > mInPos) { // stream is already open but the caller is seeking forward
      long skip = mPos - mInPos;
      if (skip < mIn.available()) {
        try {
          //TODO(beinan): we might need check the max skip length
          if (skip == mIn.skip(skip)) {
            mInPos += skip;
            return; //able to reuse the existing stream
          }
        } catch (IOException e) {
          //ignore to create a new stream
        }
      }
    }
    closeStream();
    openStream(length);
  }

  /**
   * Opens a new stream at mPos.
   */
  private void openStream(int length) throws IOException {
    GetObjectRequest getReq = new GetObjectRequest(mBucketName, mKey);
    // If the position is 0, setting range is redundant and causes an error if the file is 0 length
    // TODO(beinan): what if pos is 0 but length is not 0 when the file is 0 length, we need check
    if (mPos > 0 || length > 0) {
      getReq.setRange(mPos, mPos + length);
    }
    AmazonS3Exception lastException = null;
    String errorMessage = String.format("Failed to open key: %s bucket: %s, left retry:%d",
        mKey, mBucketName, mRetryPolicy.getAttemptCount());
    while (mRetryPolicy.attempt()) {
      try {
        mIn = getClient().getObject(getReq).getObjectContent();
        mInPos = mPos;
        return;
      } catch (AmazonS3Exception e) {
        errorMessage = String
            .format("Failed to open key: %s bucket: %s attempts: %d error: %s", mKey, mBucketName,
                mRetryPolicy.getAttemptCount(), e.getMessage());
        throw AlluxioS3Exception.from(errorMessage, e);
      }
    }
  }

  /**
   * @return the client
   */
  protected AmazonS3 getClient() {
    return mClient;
  }

  /**
   * Closes the current stream.
   */
  // TODO(calvin): Investigate if close instead of abort will bring performance benefits.
  private void closeStream() {
    if (mIn == null) {
      return;
    }
    mIn.abort();
    mIn = null;
  }
}
