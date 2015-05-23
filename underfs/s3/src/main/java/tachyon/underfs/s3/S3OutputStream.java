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
import java.security.DigestOutputStream;
import java.security.MessageDigest;
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

  /** Bucket name of the Tachyon S3 bucket */
  private final String mBucketName;
  /** Key of the file when it is uploaded to S3 */
  private final String mKey;
  /** The local file that will be uploaded when the stream is closed */
  private final File mFile;
  /** The JetS3t client for S3 operations */
  private final S3Service mClient;

  /** The outputstream to a local file where the file will be buffered until closed */
  private OutputStream mLocalOutputStream;
  /** The MD5 hash of the file */
  private MessageDigest mHash;

  /** Flag to indicate this stream has been closed, to ensure close is only done once */
  private boolean mClosed;

  public S3OutputStream(String bucketName, String key, S3Service client) throws IOException {
    Preconditions.checkArgument(bucketName != null && !bucketName.isEmpty(), "Bucket name must "
        + "not be null or empty.");
    mBucketName = bucketName;
    mKey = key;
    mClient = client;
    mFile = new File(CommonUtils.concatPath("/tmp", UUID.randomUUID()));
    try {
      mHash = MessageDigest.getInstance("MD5");
      mLocalOutputStream =
          new BufferedOutputStream(new DigestOutputStream(new FileOutputStream(mFile), mHash));
    } catch (NoSuchAlgorithmException nsae) {
      LOG.warn("Algorithm not available for MD5 hash.", nsae);
      mHash = null;
      mLocalOutputStream = new BufferedOutputStream(new FileOutputStream(mFile));
    }
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
      S3Object obj = new S3Object(mKey);
      obj.setBucketName(mBucketName);
      obj.setDataInputFile(mFile);
      obj.setContentLength(mFile.length());
      obj.setContentEncoding(Mimetypes.MIMETYPE_BINARY_OCTET_STREAM);
      if (mHash != null) {
        obj.setMd5Hash(mHash.digest());
      } else {
        LOG.warn("MD5 was not computed for: " + mKey);
      }
      mClient.putObject(mBucketName, obj);
      mFile.delete();
    } catch (ServiceException se) {
      LOG.error("Failed to upload " + mKey + ". Temporary file @ " + mFile.getPath());
      throw new IOException(se);
    }
  }
}
