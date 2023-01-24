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

package alluxio.underfs.gcs.v2;

import alluxio.underfs.UnderFileSystemOutputStream;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for writing a file into GCS using Google cloud API (GCS output stream version 2).
 * The data is streaming writing to GCS without waiting for the complete file
 * to arrive in Alluxio worker.
 */
@NotThreadSafe
public final class GCSV2OutputStream extends OutputStream implements UnderFileSystemOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(GCSV2OutputStream.class);

  /** Bucket name of the Alluxio GCS bucket. */
  private final String mBucketName;

  /** Key of the file when it is uploaded to GCS. */
  private final String mKey;

  /** The Google cloud storage client. */
  private final Storage mClient;

  /** The pre-allocated buffer for single byte write operation. */
  private final ByteBuffer mSingleByteBuffer;

  /** The blob information of the key. */
  private final BlobInfo mBlobInfo;

  /** The write channel of Google storage object. */
  private WriteChannel mWriteChannel;

  /** The MD5 hash of the file. */
  private MessageDigest mHash;

  /** Flag to indicate this stream has been closed, to ensure close is only done once. */
  private AtomicBoolean mClosed = new AtomicBoolean(false);

  /**
   * Constructs a new stream for writing a file.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the Google cloud storage client
   */
  public GCSV2OutputStream(String bucketName, String key, Storage client) {
    Preconditions.checkArgument(bucketName != null && !bucketName.isEmpty(), "Bucket name must "
        + "not be null or empty.");
    mBucketName = bucketName;
    mKey = key;
    mClient = client;
    mSingleByteBuffer = ByteBuffer.allocate(1);
    mBlobInfo = BlobInfo.newBuilder(BlobId.of(mBucketName, mKey)).build();
    try {
      mHash = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("Algorithm not available for MD5 hash.", e);
      mHash = null;
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (mHash != null) {
      mHash.update((byte) b);
    }
    if (mWriteChannel == null) {
      createWriteChannel();
    }
    mSingleByteBuffer.clear();
    mSingleByteBuffer.putInt(b);

    try {
      mWriteChannel.write(mSingleByteBuffer);
    } catch (StorageException e) {
      throw new IOException(String
          .format("Failed to write to a channel of %s in %s", mKey, mBucketName), e);
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (mHash != null) {
      mHash.update(b, off, len);
    }
    if (mWriteChannel == null) {
      createWriteChannel();
    }
    ByteBuffer buffer = ByteBuffer.wrap(b, off, len);
    try {
      mWriteChannel.write(buffer);
    } catch (StorageException e) {
      throw new IOException(String
          .format("Failed to write to a channel of %s in %s", mKey, mBucketName), e);
    }
  }

  @Override
  public void flush() throws IOException {
    // Google storage write channel does not support flush operation
  }

  @Override
  public void close() throws IOException {
    if (mClosed.getAndSet(true)) {
      return;
    }
    try {
      if (mWriteChannel != null) {
        mWriteChannel.close();
      } else {
        Blob blob = mClient.create(mBlobInfo);
        if (blob == null) {
          throw new IOException(String
              .format("Failed to create empty object %s in %s", mKey, mBucketName));
        }
      }
    } catch (ClosedChannelException e) {
      LOG.error("Channel already closed, possible duplicate close call.", e);
    } catch (IOException e) {
      LOG.error("Failed to upload {} to {}", mKey, mBucketName, e);
      throw e;
    }
  }

  private void createWriteChannel() throws IOException {
    try {
      mWriteChannel = mClient.writer(mBlobInfo);
    } catch (StorageException e) {
      throw new IOException(String
          .format("Failed to create write channel of %s in %s", mKey, mBucketName), e);
    }
  }

  @Override
  public Optional<String> getContentHash() {
    if (mHash != null) {
      return Optional.of(Base64.getEncoder().encodeToString(mHash.digest()));
    }
    return Optional.empty();
  }
}
