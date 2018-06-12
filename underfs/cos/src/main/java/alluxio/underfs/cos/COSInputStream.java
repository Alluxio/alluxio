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

import alluxio.underfs.MultiRangeObjectInputStream;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.model.COSObject;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.model.ObjectMetadata;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A stream for reading a file from COS. This input stream returns 0 when calling read with an empty
 * buffer.
 */
@NotThreadSafe
public class COSInputStream extends MultiRangeObjectInputStream {

  /** Bucket name of the Alluxio COS bucket. */
  private final String mBucketName;

  /** Key of the file in COS to read. */
  private final String mKey;

  /** The COS client for COS operations. */
  private final COSClient mCosClient;

  /** The size of the object in bytes. */
  private final long mContentLength;

  /**
   * Creates a new instance of {@link COSInputStream}.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the client for COS
   */
  COSInputStream(String bucketName, String key, COSClient client) throws IOException {
    this(bucketName, key, client, 0L);
  }

  /**
   * Creates a new instance of {@link COSInputStream}.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the client for COS
   * @param position the position to begin reading from
   */
  COSInputStream(String bucketName, String key, COSClient client, long position)
      throws IOException {
    mBucketName = bucketName;
    mKey = key;
    mCosClient = client;
    mPos = position;
    ObjectMetadata meta = mCosClient.getObjectMetadata(mBucketName, key);
    mContentLength = meta == null ? 0 : meta.getContentLength();
  }

  @Override
  protected InputStream createStream(long startPos, long endPos) throws IOException {
    GetObjectRequest req = new GetObjectRequest(mBucketName, mKey);
    // COS returns entire object if we read past the end
    req.setRange(startPos, endPos < mContentLength ? endPos - 1 : mContentLength - 1);
    COSObject object = mCosClient.getObject(req);
    return new BufferedInputStream(object.getObjectContent());
  }
}
