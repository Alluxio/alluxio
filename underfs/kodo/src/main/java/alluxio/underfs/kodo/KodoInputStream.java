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

package alluxio.underfs.kodo;

import alluxio.exception.status.NotFoundException;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.MultiRangeObjectInputStream;

import com.qiniu.common.QiniuException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * A stream for reading a file from Kodo. This input stream returns 0 when calling read with an
 * empty buffer.
 */
public class KodoInputStream extends MultiRangeObjectInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(KodoInputStream.class);

  /**
   * Key of the file in Kodo to read.
   */
  private final String mKey;

  /**
   * The Kodo client for Kodo operations.
   */
  private final KodoClient mKodoclent;

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
   * Constructor for an input stream to an object in Kodo.
   *
   * @param key the key of the object
   * @param kodoClient the Kodo client
   * @param position the position to begin reading from
   * @param retryPolicy retry policy in case the key does not exist
   * @param multiRangeChunkSize the chunk size to use on this stream
   */
  KodoInputStream(String key, KodoClient kodoClient, long position,
      RetryPolicy retryPolicy, long multiRangeChunkSize) throws QiniuException {
    super(multiRangeChunkSize);
    mKey = key;
    mKodoclent = kodoClient;
    mPos = position;
    mRetryPolicy = retryPolicy;
    mContentLength = kodoClient.getFileInfo(key).fsize;
  }

  @Override
  protected InputStream createStream(long startPos, long endPos)
      throws IOException {
    IOException lastException = null;
    String errorMessage = String.format("Failed to open key: %s", mKey);
    while (mRetryPolicy.attempt()) {
      try {
        return mKodoclent.getObject(mKey, startPos, endPos, mContentLength);
      } catch (NotFoundException e) {
        errorMessage = String
            .format("Failed to open key: %s attempts: %s error: %s", mKey,
                mRetryPolicy.getAttemptCount(), e.getMessage());
        // Key does not exist
        lastException = e;
      }
    }
    // Failed after retrying key does not exist
    throw new IOException(errorMessage, lastException);
  }
}

