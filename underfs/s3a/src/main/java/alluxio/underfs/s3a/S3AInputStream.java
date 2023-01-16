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

import alluxio.Seekable;
import alluxio.exception.PreconditionMessage;
import alluxio.retry.RetryPolicy;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * An S3A input stream that supports skip and seek efficiently.
 * Recommended wrap around an BufferedInputStream or
 * {@link alluxio.file.SeekableBufferedInputStream} to improve performance.
 */
@NotThreadSafe
public class S3AInputStream extends InputStream implements Seekable {
  /** Client for operations with s3. */
  protected AmazonS3 mClient;
  /** Name of the bucket the object resides in. */
  protected final String mBucketName;
  /** The path of the object to read. */
  protected final String mKey;
  protected final byte[] mSingleByteHolder = new byte[1];
  protected final GetObjectRequest mReadRequest;
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
    Preconditions.checkArgument(offset >= 0 && length >= 0 && offset + length <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, offset, length);
    if (length == 0) {
      return 0;
    }
    S3Object object = null;
    AlluxioS3Exception lastException = null;
    do {
      try {
        // Range check approach: set range (inclusive start, inclusive end)
        // start: should be < file length, error out otherwise
        //        e.g. error out when start == 0 && fileLength == 0
        //        start < 0, read all
        // end: if start > end, read all
        //      if start <= end < file length, read from start to end
        //      if end >= file length, read from start to file length - 1
        mReadRequest.setRange(mPos, mPos + length - 1);
        object = getClient().getObject((mReadRequest));
        break;
      } catch (AmazonS3Exception e) {
        if (e.getStatusCode() == 416) {
          // InvalidRange exception when mPos >= file length
          return -1;
        }
        String errorMessage = String
            .format("Failed to get object: %s bucket: %s attempts: %d error: %s", mKey, mBucketName,
                mRetryPolicy.getAttemptCount(), e.getMessage());
        lastException = AlluxioS3Exception.from(errorMessage, e);
        if (!lastException.isRetryable()) {
          throw lastException;
        }
      }
    } while (mRetryPolicy.attempt());
    if (object == null) {
      Preconditions.checkNotNull(lastException,
          "s3 object must be achieved or exception is thrown");
      throw lastException;
    }
    try (S3ObjectInputStream in = object.getObjectContent()) {
      int currentRead = 0;
      int totalRead = 0;
      while (totalRead < length) {
        currentRead = in.read(b, offset + totalRead, length - totalRead);
        if (currentRead <= 0) {
          break;
        }
        totalRead += currentRead;
      }
      mPos += totalRead;
      return totalRead == 0 ? currentRead : totalRead;
    }
  }

  @Override
  public long skip(long n) {
    if (n <= 0) {
      return 0;
    }
    mPos += n;
    return n;
  }

  @Override
  public long getPos() {
    return mPos;
  }

  @Override
  public void seek(long pos) {
    Preconditions.checkArgument(pos >= 0, "Seek position is negative: %s", pos);
    mPos = pos;
  }

  /**
   * @return the client
   */
  protected AmazonS3 getClient() {
    return mClient;
  }
}
