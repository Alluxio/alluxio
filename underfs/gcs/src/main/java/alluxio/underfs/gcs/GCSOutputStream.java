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

package alluxio.underfs.gcs;

import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.GoogleStorageService;
import org.jets3t.service.model.GSObject;
import org.jets3t.service.utils.Mimetypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for writing a file into GCS. The data will be persisted to a temporary directory on the
 * local disk and copied as a complete file when the {@link #close()} method is called.
 */
@NotThreadSafe
public final class GCSOutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(GCSOutputStream.class);

  /** Bucket name of the Alluxio GCS bucket. */
  private final String mBucketName;

  /** Key of the file when it is uploaded to GCS. */
  private final String mKey;

  /** The local file that will be uploaded when the stream is closed. */
  private final File mFile;

  /** The JetS3t client for GCS operations. */
  private final GoogleStorageService mClient;

  /** The output stream to a local file where the file will be buffered until closed. */
  private OutputStream mLocalOutputStream;

  /** The MD5 hash of the file. */
  private MessageDigest mHash;

  /** Flag to indicate this stream has been closed, to ensure close is only done once. */
  private AtomicBoolean mClosed = new AtomicBoolean(false);

  /**
   * Constructs a new stream for writing a file.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the JetS3t client
   * @param tmpDirs a list of temporary directories
   */
  public GCSOutputStream(String bucketName, String key, GoogleStorageService client,
      List<String> tmpDirs) throws IOException {
    Preconditions.checkArgument(bucketName != null && !bucketName.isEmpty(), "Bucket name must "
        + "not be null or empty.");
    mBucketName = bucketName;
    mKey = key;
    mClient = client;
    mFile = new File(PathUtils.concatPath(CommonUtils.getTmpDir(tmpDirs), UUID.randomUUID()));
    try {
      mHash = MessageDigest.getInstance("MD5");
      mLocalOutputStream =
          new BufferedOutputStream(new DigestOutputStream(new FileOutputStream(mFile), mHash));
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("Algorithm not available for MD5 hash.", e);
      mHash = null;
      mLocalOutputStream = new BufferedOutputStream(new FileOutputStream(mFile));
    }
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
    if (mClosed.getAndSet(true)) {
      return;
    }
    mLocalOutputStream.close();
    try {
      GSObject obj = new GSObject(mKey);
      obj.setBucketName(mBucketName);
      obj.setDataInputFile(mFile);
      obj.setContentLength(mFile.length());
      obj.setContentType(Mimetypes.MIMETYPE_BINARY_OCTET_STREAM);
      if (mHash != null) {
        obj.setMd5Hash(mHash.digest());
      } else {
        LOG.warn("MD5 was not computed for: {}", mKey);
      }
      mClient.putObject(mBucketName, obj);
    } catch (ServiceException e) {
      LOG.error("Failed to upload {}.", mKey);
      throw new IOException(e);
    } finally {
      // Delete the temporary file on the local machine if the GCS client completed the
      // upload or if the upload failed.
      if (!mFile.delete()) {
        LOG.error("Failed to delete temporary file @ {}", mFile.getPath());
      }
    }
  }
}

