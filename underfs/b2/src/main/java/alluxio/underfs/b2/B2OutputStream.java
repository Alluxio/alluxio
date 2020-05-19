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

package alluxio.underfs.b2;

import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.contentSources.B2ContentSource;
import com.backblaze.b2.client.contentSources.B2ContentTypes;
import com.backblaze.b2.client.contentSources.B2FileContentSource;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2UploadFileRequest;
import com.backblaze.b2.client.structures.B2UploadListener;
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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A stream for writing a file to B2.
 */
public class B2OutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(B2OutputStream.class);

  /**
   * The B2 storage client for Backblaze B2 operations.
   */
  private final B2StorageClient mB2StorageClient;

  /**
   * Bucket id of the Alluxio B2 bucket.
   */
  private final String mBucketId;

  /**
   * Flag to indicate this stream has been closed, to ensure close is only done once.
   */
  private final AtomicBoolean mClosed = new AtomicBoolean(false);

  /**
   * The MD5 hash of the file.
   */
  private MessageDigest mHash;

  /**
   * Key of the file when it is uploaded to Backblaze B2.
   */
  private final String mKey;

  /**
   * The local file that will be uploaded when the stream is closed.
   */
  private final File mFile;

  /**
   * The output stream to a local file where the file will be buffered until closed.
   */
  private OutputStream mLocalOutputStream;

  /**
   * Constructor for an output stream to write on Backblaze B2 using the B2-sdk.
   *
   * @param bucketId the bucket to write datae
   * @param key the path of the object to be written
   * @param b2StorageClient the b2 client to use for operations
   * @param tmpDirs the temporary directory to store the file before uploading to under fs
   */
  public B2OutputStream(final String bucketId, final String key,
      final B2StorageClient b2StorageClient, final List<String> tmpDirs) throws IOException {
    Preconditions.checkArgument(bucketId != null && !bucketId.isEmpty(),
        "Bucket name must " + "not be null or empty.");

    mBucketId = bucketId;
    mKey = key;
    mB2StorageClient = b2StorageClient;
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
  public void write(final int b) throws IOException {
    mLocalOutputStream.write(b);
  }

  @Override
  public void write(final byte[] b) throws IOException {
    mLocalOutputStream.write(b, 0, b.length);
  }

  @Override
  public void write(final byte[] b, final int off, final int len) throws IOException {
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
      final B2UploadListener uploadListener = (progress) -> {
        final double percent = (100. * (progress.getBytesSoFar() / (double) progress.getLength()));
        LOG.debug(String.format("  progress(%3.2f, %s)", percent, progress.toString()));
      };
      final B2ContentSource source = B2FileContentSource.builder(mFile).build();
      B2UploadFileRequest request =
          B2UploadFileRequest.builder(mBucketId, mKey, B2ContentTypes.APPLICATION_OCTET, source)
              .setListener(uploadListener).build();

      mB2StorageClient.uploadSmallFile(request);
    } catch (B2Exception e) {
      LOG.error("Failed to open stream with remote storage. bucket id: [{}], key: [{}]",
          mBucketId, mKey);
      throw new IOException(e);
    } finally {
      // Delete the temporary file on the local machine if the GCS client completed
      // the upload or if the upload failed.
      if (!mFile.delete()) {
        LOG.error("Failed to delete temporary file @ {}", mFile.getPath());
      }
    }
  }
}
