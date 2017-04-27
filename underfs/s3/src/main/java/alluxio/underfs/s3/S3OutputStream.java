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

package alluxio.underfs.s3;

import alluxio.Constants;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import org.jets3t.service.S3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.utils.Mimetypes;
import org.jets3t.service.utils.MultipartUtils;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for writing a file into S3. The data will be persisted to a temporary directory on the
 * local disk and copied as a complete file when the {@link #close()} method is called.
 */
@NotThreadSafe
public class S3OutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3OutputStream.class);

  /** Bucket name of the Alluxio S3 bucket. */
  private final String mBucketName;

  /** Key of the file when it is uploaded to S3. */
  private final String mKey;

  /** The local file that will be uploaded when the stream is closed. */
  private final File mFile;

  /** The JetS3t client for S3 operations. */
  private final S3Service mClient;

  /** The output stream to a local file where the file will be buffered until closed. */
  private OutputStream mLocalOutputStream;

  /** The MD5 hash of the file. */
  private MessageDigest mHash;

  /** Flag to indicate this stream has been closed, to ensure close is only done once. */
  private AtomicBoolean mClosed = new AtomicBoolean(false);

  /**
   * A {@link MultipartUtils} to upload the file to S3 using Multipart Uploads. Multipart Uploads
   * involves uploading an object's data in parts instead of all at once, which can work around S3's
   * limit of 5GB on a single Object PUT operation.
   *
   * It is recommended (http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html)
   * to upload file larger than 100MB using Multipart Uploads.
   */
  private static final MultipartUtils MULTIPART_UTIL = new MultipartUtils(Constants.MB * 100);

  /**
   * Constructs a new stream for writing a file.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the JetS3t client
   */
  public S3OutputStream(String bucketName, String key, S3Service client) throws IOException {
    Preconditions.checkArgument(bucketName != null && !bucketName.isEmpty(), "Bucket name must "
        + "not be null or empty.");
    mBucketName = bucketName;
    mKey = key;
    mClient = client;
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
      S3Object obj = new S3Object(mKey);
      obj.setBucketName(mBucketName);
      obj.setDataInputFile(mFile);
      obj.setContentLength(mFile.length());
      obj.setContentEncoding(Mimetypes.MIMETYPE_BINARY_OCTET_STREAM);
      if (mHash != null) {
        obj.setMd5Hash(mHash.digest());
      } else {
        LOG.warn("MD5 was not computed for: {}", mKey);
      }
      if (MULTIPART_UTIL.isFileLargerThanMaxPartSize(mFile)) {
        // Big object will be split into parts and uploaded to S3 in parallel.
        List<StorageObject> objectsToUploadAsMultipart = new ArrayList<>();
        objectsToUploadAsMultipart.add(obj);
        MULTIPART_UTIL.uploadObjects(mBucketName, mClient, objectsToUploadAsMultipart, null);
      } else {
        // Avoid uploading file with Multipart if it's not necessary to save the
        // extra overhead.
        mClient.putObject(mBucketName, obj);
      }
      if (!mFile.delete()) {
        LOG.error("Failed to delete temporary file @ {}", mFile.getPath());
      }
    } catch (Exception e) {
      LOG.error("Failed to upload {}. Temporary file @ {}", mKey, mFile.getPath());
      throw new IOException(e);
    }
  }
}
