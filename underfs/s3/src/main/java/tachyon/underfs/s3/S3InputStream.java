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

package tachyon.underfs.s3;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;

public class S3InputStream extends InputStream {

  private final String mBucketName;
  private final String mKey;
  private final S3Service mClient;

  private S3Object mObject;
  private BufferedInputStream mInputStream;
  private long mPos;

  S3InputStream(String bucketName, String key, S3Service client) throws ServiceException {
    mBucketName = bucketName;
    mKey = key;
    mClient = client;
    mObject = mClient.getObject(mBucketName, mKey);
    mInputStream = new BufferedInputStream(mObject.getDataInputStream());
  }

  public int read() throws IOException {
    int ret = mInputStream.read();
    if (ret != -1) {
      mPos++;
    }
    return ret;
  }

  public int read(byte[] b, int off, int len) throws IOException {
    int ret = mInputStream.read(b, off, len);
    if (ret != -1) {
      mPos += ret;
    }
    return ret;
  }

  @Override
  public long skip(long n) throws IOException {
    if (mInputStream.available() >= n) {
      return mInputStream.skip(n);
    }
    mPos += n;
    try {
      mObject = mClient.getObject(mBucketName, mKey, null, null, null, null, mPos, null);
      mInputStream = new BufferedInputStream(mObject.getDataInputStream());
    } catch (ServiceException se) {
      throw new IOException(se);
    }
    return n;
  }
}
