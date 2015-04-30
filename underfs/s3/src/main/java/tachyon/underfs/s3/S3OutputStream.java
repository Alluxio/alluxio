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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.utils.Mimetypes;

public class S3OutputStream extends OutputStream {
  private final String mBucketName;
  private final String mKey;
  private final OutputStream mOut;
  private final File mFile;
  private final S3Service mClient;

  public S3OutputStream(String bucketName, String key, S3Service client) throws IOException {
    mBucketName = bucketName;
    mKey = key;
    mClient = client;
    mFile = new File("/tmp/" + key);
    mOut = new FileOutputStream(mFile);
  }

  @Override
  public void write(int b) throws IOException {
    mOut.write(b);
  }

  @Override
  public void flush() throws IOException {
    mOut.flush();
  }

  @Override
  public void close() throws IOException {
    mOut.close();
    S3Object obj = new S3Object(mKey);
    obj.setDataInputFile(mFile);
    obj.setContentLength(mFile.length());
    obj.setContentEncoding(Mimetypes.MIMETYPE_BINARY_OCTET_STREAM);
    try {
      mClient.putObject(mBucketName, obj);
    } catch (ServiceException se) {
      throw new IOException(se);
    }
  }
}
