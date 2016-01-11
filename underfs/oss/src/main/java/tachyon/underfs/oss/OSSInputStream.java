/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.underfs.oss;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObject;

/**
 * This class is a buffered wrapper around the input stream returned by
 * {@link OSSObject#getObjectContent()}.
 * This input stream return 0 when calling read with an empty buffer.
 */
public class OSSInputStream extends InputStream {

  /** Bucket name of the Tachyon OSS bucket */
  private final String mBucketName;
  /** Key of the file in OSS to read */
  private final String mKey;
  /** The OSS client for OSS operations */
  private final OSSClient mOssClient;

  /** The storage object that will be updated on each large skip */
  private OSSObject mObject;
  /** The underlying input stream */
  private BufferedInputStream mInputStream;

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
