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
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.model.ObjectMetadata;
import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.util.io.PathUtils;

/**
 * A stream for writing a file into OSS. The data will be persisted to a temporary directory on the
 * local disk and copied as a complete file when the {@link #close()} method is called.
 */
public final class OSSOutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Bucket name of the Tachyon OSS bucket. */
  private final String mBucketName;
  /** Key of the file when it is uploaded to OSS. */
  private final String mKey;
  /** The local file that will be uploaded when the stream is closed. */
  private final File mFile;
  /** The oss client for OSS operations. */
  private final OSSClient mOssClient;

  /** The outputstream to a local file where the file will be buffered until closed. */
  private OutputStream mLocalOutputStream;
  /** The MD5 hash of the file. */
  private MessageDigest mHash;

  /** Flag to indicate this stream has been closed, to ensure close is only done once. */
  private boolean mClosed;

  /**
   * Creates a name instance of {@link OSSOutputStream}.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the client for OSS
   * @throws IOException if an I/O error occurs
   */
  public OSSOutputStream(String bucketName, String key, OSSClient client) throws IOException {
    Preconditions.checkArgument(bucketName != null && !bucketName.isEmpty(),
        "Bucket name must not be null or empty.");
    Preconditions.checkArgument(key != null && !key.isEmpty(),
        "OSS path must not be null or empty.");
    Preconditions.checkArgument(client != null, "OSSClient must not be null.");
    mBucketName = bucketName;
    mKey = key;
    mOssClient = client;

    mFile = new File(PathUtils.concatPath("/tmp", UUID.randomUUID()));

    try {
      mHash = MessageDigest.getInstance("MD5");
      mLocalOutputStream =
          new BufferedOutputStream(new DigestOutputStream(new FileOutputStream(mFile), mHash));
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("Algorithm not available for MD5 hash.", e);
      mHash = null;
      mLocalOutputStream = new BufferedOutputStream(new FileOutputStream(mFile));
    }
    mClosed = false;
  }

  /**
   * Writes the given bytes to this output stream. Before close, the bytes are all written to local
   * file.
   *
   * @param b the bytes to write
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void write(int b) throws IOException {
    mLocalOutputStream.write(b);
  }

  /**
   * Writes the given byte array to this output stream. Before close, the bytes are all written to
   * local file.
   *
   * @param b the byte array
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void write(byte[] b) throws IOException {
    mLocalOutputStream.write(b, 0, b.length);
  }

  /**
   * Writes the given number of bytes from the given byte array starting at the given offset to this
   * output stream. Before close, the bytes are all written to local file.
   *
   * @param b the byte array
   * @param off the start offset in the data
   * @param len the number of bytes to write
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mLocalOutputStream.write(b, off, len);
  }

  /**
   * Flushes this output stream and forces any buffered output bytes to be written out. Before
   * close, the data are flushed to local file.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void flush() throws IOException {
    mLocalOutputStream.flush();
  }

  /**
   * Closes this output stream. When an output stream is closed, the local temporary file is
   * uploaded to OSS Service. Once the file is uploaded, the temporary file is deleted.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mLocalOutputStream.close();
    try {
      BufferedInputStream in = new BufferedInputStream(
          new FileInputStream(mFile));
      ObjectMetadata objMeta = new ObjectMetadata();
      objMeta.setContentLength(mFile.length());
      if (mHash != null) {
        byte[] hashBytes = mHash.digest();
        objMeta.setContentMD5(new String(Base64.encodeBase64(hashBytes)));
      }
      mOssClient.putObject(mBucketName, mKey, in, objMeta);
      mFile.delete();
    } catch (ServiceException e) {
      LOG.error("Failed to upload {}. Temporary file @ {}", mKey, mFile.getPath());
      throw new IOException(e);
    }
  }
}
