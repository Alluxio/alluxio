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

package alluxio.underfs.tos;

import alluxio.retry.RetryPolicy;
import alluxio.underfs.MultiRangeObjectInputStream;

import com.volcengine.tos.TOSV2;
import com.volcengine.tos.TosException;
import com.volcengine.tos.model.object.GetObjectV2Input;
import com.volcengine.tos.model.object.GetObjectV2Output;
import com.volcengine.tos.model.object.HeadObjectV2Input;
import com.volcengine.tos.model.object.HeadObjectV2Output;
import com.volcengine.tos.model.object.ObjectMetaRequestOptions;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for reading a file from TOS. This input stream returns 0 when calling read with an empty
 * buffer.
 */
@NotThreadSafe
public class TOSInputStream extends MultiRangeObjectInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(TOSInputStream.class);

  /**
   * Bucket name of the Alluxio TOS bucket.
   */
  private final String mBucketName;

  /**
   * Key of the file in TOS to read.
   */
  private final String mKey;

  /**
   * The TOS client for TOS operations.
   */
  private final TOSV2 mTosClient;

  /**
   * The size of the object in bytes.
   */
  private final long mContentLength;

  /**
   * Policy determining the retry behavior in case the key does not exist. The key may not exist
   * because of eventual consistency.
   */
  private final RetryPolicy mRetryPolicy;

  /**
   * Creates a new instance of {@link TOSInputStream}.
   *
   * @param bucketName          the name of the bucket
   * @param key                 the key of the file
   * @param client              the client for COS
   * @param multiRangeChunkSize the chunk size to use on this stream
   */
  TOSInputStream(String bucketName, String key, TOSV2 client, RetryPolicy retryPolicy,
                 long multiRangeChunkSize) throws IOException {
    this(bucketName, key, client, 0L, retryPolicy, multiRangeChunkSize);
  }

  /**
   * Creates a new instance of {@link TOSInputStream}.
   *
   * @param bucketName          the name of the bucket
   * @param key                 the key of the file
   * @param client              the client for TOS
   * @param position            the position to begin reading from
   * @param multiRangeChunkSize the chunk size to use on this stream
   */
  TOSInputStream(String bucketName, String key, TOSV2 client, long position,
                 RetryPolicy retryPolicy, long multiRangeChunkSize) throws IOException {
    super(multiRangeChunkSize);
    mBucketName = bucketName;
    mKey = key;
    mTosClient = client;
    mPos = position;
    mRetryPolicy = retryPolicy;
    HeadObjectV2Input input = new HeadObjectV2Input().setBucket(bucketName).setKey(key);
    HeadObjectV2Output meta = mTosClient.headObject(input);
    mContentLength = meta == null ? 0 : meta.getContentLength();
  }

  @Override
  protected InputStream createStream(long startPos, long endPos)
      throws IOException {
    ObjectMetaRequestOptions options = new ObjectMetaRequestOptions();
    GetObjectV2Input req = new GetObjectV2Input().setBucket(mBucketName).setKey(mKey);
    // TOS returns entire object if we read past the end
    options.setRange(startPos, endPos < mContentLength ? endPos - 1 : mContentLength - 1);
    req.setOptions(options);
    TosException lastException = null;
    String errorMessage = String.format("Failed to open key: %s bucket: %s", mKey, mBucketName);
    RetryPolicy retryPolicy = mRetryPolicy.copy();
    while (retryPolicy.attempt()) {
      try {
        GetObjectV2Output object = mTosClient.getObject(req);
        return new BufferedInputStream(object.getContent());
      } catch (TosException e) {
        errorMessage = String
            .format("Failed to open key: %s bucket: %s attempts: %d error: %s", mKey, mBucketName,
                retryPolicy.getAttemptCount(), e.getMessage());
        if (e.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
          throw new IOException(errorMessage, e);
        }
        // Key does not exist
        lastException = e;
      }
    }
    // Failed after retrying key does not exist
    throw new IOException(errorMessage, lastException);
  }
}
