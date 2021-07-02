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

package alluxio.underfs.s3a;

import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.util.Base64;
import com.google.common.base.Preconditions;
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

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for writing a file into S3. The data will be persisted to a temporary directory on the
 * local disk and copied as a complete file when the {@link #close()} method is called. The data
 * transfer is done using a {@link TransferManager} which manages the upload threads and handles
 * multipart upload.
 */
@NotThreadSafe
public class S3AOutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3AOutputStream.class);

  private final boolean mSseEnabled;

  /** Bucket name of the Alluxio S3 bucket. */
  private final String mBucketName;

  /** Key of the file when it is uploaded to S3. */
  private final String mKey;

  /** The local file that will be uploaded when the stream is closed. */
  private final File mFile;

  /** Flag to indicate this stream has been closed, to ensure close is only done once. */
  private boolean mClosed = false;

  /**
   * A {@link TransferManager} to upload the file to S3 using Multipart Uploads. Multipart Uploads
   * involves uploading an object's data in parts instead of all at once, which can work around S3's
   * limit of 5GB on a single Object PUT operation.
   *
   * It is recommended (http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html)
   * to upload file larger than 100MB using Multipart Uploads.
   */
  // TODO(calvin): Investigate if the lower level API can be more efficient.
  private final TransferManager mManager;

  /** The output stream to a local file where the file will be buffered until closed. */
  private OutputStream mLocalOutputStream;

  /** The MD5 hash of the file. */
  private MessageDigest mHash;

  /**
   * Constructs a new stream for writing a file.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param manager the transfer manager to upload the file with
   * @param tmpDirs a list of temporary directories
   * @param sseEnabled whether or not server side encryption is enabled
   */
  public S3AOutputStream(String bucketName, String key, TransferManager manager,
      List<String> tmpDirs, boolean sseEnabled) throws IOException {
    Preconditions.checkArgument(bucketName != null && !bucketName.isEmpty(), "Bucket name must "
        + "not be null or empty.");
    mBucketName = bucketName;
    mKey = key;
    mManager = manager;
    mSseEnabled = sseEnabled;
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
    if (mClosed) {
      return;
    }
    mLocalOutputStream.close();
    String path = getUploadPath();
    try {
      // Generate the object metadata by setting server side encryption, md5 checksum, the file
      // length, and encoding as octet stream since no assumptions are made about the file type
      ObjectMetadata meta = new ObjectMetadata();
      if (mSseEnabled) {
        meta.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
      }
      if (mHash != null) {
        meta.setContentMD5(new String(Base64.encode(mHash.digest())));
      }
      meta.setContentLength(mFile.length());
      meta.setContentType(Mimetypes.MIMETYPE_OCTET_STREAM);

      // Generate the put request and wait for the transfer manager to complete the upload
      PutObjectRequest putReq = new PutObjectRequest(mBucketName, path, mFile).withMetadata(meta);
      mManager.upload(putReq).waitForUploadResult();
    } catch (Exception e) {
      LOG.error("Failed to upload {}", path, e);
      throw new IOException(e);
    } finally {
      // Delete the temporary file on the local machine if the transfer manager completed the
      // upload or if the upload failed.
      if (!mFile.delete()) {
        LOG.error("Failed to delete temporary file @ {}", mFile.getPath());
      }
      // Set the closed flag, close can be retried until mFile.delete is called successfully
      mClosed = true;
    }
  }

  /**
   * @return the path in S3 to upload the file to
   */
  protected String getUploadPath() {
    return mKey;
  }
}
