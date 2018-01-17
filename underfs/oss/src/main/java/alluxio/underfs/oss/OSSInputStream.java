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

package alluxio.underfs.oss;

import alluxio.underfs.MultiRangeObjectInputStream;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for reading a file from OSS. This input stream returns 0 when calling read with an empty
 * buffer.
 */
@NotThreadSafe
public class OSSInputStream extends MultiRangeObjectInputStream {

  /** Bucket name of the Alluxio OSS bucket. */
  private final String mBucketName;

  /** Key of the file in OSS to read. */
  private final String mKey;

  /** The OSS client for OSS operations. */
  private final OSSClient mOssClient;

  /** The size of the object in bytes. */
  private final long mContentLength;

  /**
   * Creates a new instance of {@link OSSInputStream}.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the client for OSS
   */
  OSSInputStream(String bucketName, String key, OSSClient client) throws IOException {
    this(bucketName, key, client, 0L);
  }

  /**
   * Creates a new instance of {@link OSSInputStream}.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the client for OSS
   * @param position the position to begin reading from
   */
  OSSInputStream(String bucketName, String key, OSSClient client, long position)
      throws IOException {
    mBucketName = bucketName;
    mKey = key;
    mOssClient = client;
    mPos = position;
    ObjectMetadata meta = mOssClient.getObjectMetadata(mBucketName, key);
    mContentLength = meta == null ? 0 : meta.getContentLength();
  }

  @Override
  protected InputStream createStream(long startPos, long endPos) throws IOException {
    GetObjectRequest req = new GetObjectRequest(mBucketName, mKey);
    // OSS returns entire object if we read past the end
    req.setRange(startPos, endPos < mContentLength ? endPos - 1 : mContentLength - 1);
    OSSObject ossObject = mOssClient.getObject(req);
    return new BufferedInputStream(ossObject.getObjectContent());
  }
}
