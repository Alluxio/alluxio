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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import com.google.common.base.Preconditions;

import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.utils.Mimetypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.util.CommonUtils;

/**
 * This class creates a streaming interface for writing a file in s3. The data will be persisted
 * to a temporary directory on local disk and copied as a complete file when the close() method is
 * called.
 */
public class S3OutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final String mBucketName;
  private final String mKey;
  private final OutputStream mLocalOutputStream;
  private final File mFile;
  private final S3Service mClient;

  private boolean mClosed;

  public S3OutputStream(String bucketName, String key, S3Service client) throws IOException {
    Preconditions.checkArgument(bucketName != null && !bucketName.isEmpty(), "Bucket name must "
        + "not be null or empty.");
    mBucketName = bucketName;
    mKey = key;
    mClient = client;
    mFile = new File(CommonUtils.concatPath("/tmp", UUID.randomUUID()));
    mLocalOutputStream = new BufferedOutputStream(new FileOutputStream(mFile));
    mClosed = false;
  }

  @Override
  public void write(int b) throws IOException {
    mLocalOutputStream.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    mLocalOutputStream.write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mLocalOutputStream.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    mLocalOutputStream.flush();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mLocalOutputStream.close();
    try {
      S3Object obj = new S3Object(mFile);
      obj.setBucketName(mBucketName);
      obj.setKey(mKey);
      obj.setContentEncoding(Mimetypes.MIMETYPE_BINARY_OCTET_STREAM);
      mClient.putObject(mBucketName, obj);
      mFile.delete();
    } catch (ServiceException se) {
      LOG.error("Failed to upload " + mKey + ". Temporary file @ " + mFile.getPath());
      throw new IOException(se);
    } catch (NoSuchAlgorithmException nsae) {
      throw new IOException(nsae);
    }
  }
}
