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

package alluxio.underfs.cos;

import alluxio.retry.RetryPolicy;
import alluxio.underfs.MultiRangeObjectInputStream;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.COSObject;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.model.ObjectMetadata;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for reading a file from COS. This input stream returns 0 when calling read with an empty
 * buffer.
 */
@NotThreadSafe
public class COSInputStream extends MultiRangeObjectInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(COSInputStream.class);

  /** Bucket name of the Alluxio COS bucket. */
  private final String mBucketName;

  /** Key of the file in COS to read. */
  private final String mKey;

  /** The COS client for COS operations. */
  private final COSClient mCosClient;

  /** The size of the object in bytes. */
  private final long mContentLength;

  /**
   * Policy determining the retry behavior in case the key does not exist. The key may not exist
   * because of eventual consistency.
   */
  private final RetryPolicy mRetryPolicy;

  /**
   * Creates a new instance of {@link COSInputStream}.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the client for COS
   * @param retryPolicy retry policy in case the key does not exist
   * @param multiRangeChunkSize the chunk size to use on this stream
   */
  COSInputStream(String bucketName, String key, COSClient client,
      RetryPolicy retryPolicy, long multiRangeChunkSize) throws IOException {
    this(bucketName, key, client, 0L, retryPolicy, multiRangeChunkSize);
  }

  /**
   * Creates a new instance of {@link COSInputStream}.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the client for COS
   * @param position the position to begin reading from
   * @param retryPolicy retry policy in case the key does not exist
   * @param multiRangeChunkSize the chunk size to use on this stream
   */
  COSInputStream(String bucketName, String key, COSClient client, long position,
      RetryPolicy retryPolicy, long multiRangeChunkSize) throws IOException {
    super(multiRangeChunkSize);
    mBucketName = bucketName;
    mKey = key;
    mCosClient = client;
    mPos = position;
    mRetryPolicy = retryPolicy;
    ObjectMetadata meta = mCosClient.getObjectMetadata(mBucketName, key);
    mContentLength = meta == null ? 0 : meta.getContentLength();
  }

  @Override
  protected InputStream createStream(long startPos, long endPos)
      throws IOException {
    GetObjectRequest req = new GetObjectRequest(mBucketName, mKey);
    // COS returns entire object if we read past the end
    req.setRange(startPos, endPos < mContentLength ? endPos - 1 : mContentLength - 1);
    CosServiceException lastException = null;
    String errorMessage = String.format("Failed to open key: %s bucket: %s", mKey, mBucketName);
    while (mRetryPolicy.attempt()) {
      try {
        COSObject object = mCosClient.getObject(req);
        return new BufferedInputStream(object.getObjectContent());
      } catch (CosServiceException e) {
        errorMessage = String
            .format("Failed to open key: %s bucket: %s attempts: %d error: %s", mKey, mBucketName,
                mRetryPolicy.getAttemptCount(), e.getMessage());
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
