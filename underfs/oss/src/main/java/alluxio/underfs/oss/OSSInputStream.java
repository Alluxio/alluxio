/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.oss;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObject;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for reading a file from OSS. This input stream returns 0 when calling read with an empty
 * buffer.
 */
@NotThreadSafe
public class OSSInputStream extends InputStream {

  /** Bucket name of the Alluxio OSS bucket. */
  private final String mBucketName;

  /** Key of the file in OSS to read. */
  private final String mKey;

  /** The OSS client for OSS operations. */
  private final OSSClient mOssClient;

  /** The storage object that will be updated on each large skip. */
  private final OSSObject mObject;

  /** The underlying input stream. */
  private final BufferedInputStream mInputStream;

  /**
   * Creates a new instance of {@link OSSInputStream}.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the client for OSS
   * @throws IOException if an I/O error occurs
   */
  OSSInputStream(String bucketName, String key, OSSClient client) throws IOException {
    mBucketName = bucketName;
    mKey = key;
    mOssClient = client;
    mObject = mOssClient.getObject(mBucketName, mKey);
    mInputStream = new BufferedInputStream(mObject.getObjectContent());
  }

  @Override
  public void close() throws IOException {
    mInputStream.close();
  }

  @Override
  public int read() throws IOException {
    int ret = mInputStream.read();
    return ret;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int ret = mInputStream.read(b, off, len);
    return ret;
  }

  @Override
  public long skip(long n) throws IOException {
    // TODO(luoli523) currently, the oss sdk doesn't support get the oss Object in a
    // special position of the stream. It will support this feature in the future.
    // Now we just read n bytes and discard to skip.
    return super.skip(n);
  }
}
