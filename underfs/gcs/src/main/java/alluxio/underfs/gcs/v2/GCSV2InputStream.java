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

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for reading a file from GCS using Google cloud API (GCS input stream version 2).
 * The main purpose is to provide a faster skip method, as the underlying implementation
 * will read and discard bytes until the number to skip has been reached.
 * This input stream returns 0 when calling read with an empty buffer.
 */
@NotThreadSafe
public final class GCSV2InputStream extends InputStream {
  /** Bucket name of the Alluxio GCS bucket. */
  private final String mBucketName;

  /** Key of the file in GCS to read. */
  private final String mKey;

  /** The Google cloud storage client. */
  private final Storage mClient;

  /** The pre-allocated buffer for single byte read operation. */
  private final ByteBuffer mSingleByteBuffer;

  /** The underlying input stream. */
  private ReadChannel mReadChannel;

  /** Position of the stream. */
  private long mPos;

  /**
   * Creates a new instance of {@link GCSV2InputStream}, at a specific position.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the Google cloud storage client
   * @param pos the position to start
   */
  GCSV2InputStream(String bucketName, String key, Storage client, long pos) {
    mBucketName = bucketName;
    mKey = key;
    mClient = client;
    mPos = pos;
    mSingleByteBuffer = ByteBuffer.allocate(1);
  }

  @Override
  public void close() throws IOException {
    if (mReadChannel != null) {
      mReadChannel.close();
    }
  }

  @Override
  public int read() throws IOException {
    if (mReadChannel == null) {
      openStream();
    }
    mSingleByteBuffer.clear();
    int num = mReadChannel.read(mSingleByteBuffer);
    if (num != -1) { // valid data read
      mPos++;
    } else {
      return -1;
    }
    mSingleByteBuffer.position(0);
    return mSingleByteBuffer.get() & 0xff;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    if (mReadChannel == null) {
      openStream();
    }
    int ret = mReadChannel.read(ByteBuffer.wrap(b, off, len));
    if (ret != -1) {
      mPos += ret;
    }
    return ret;
  }

  /**
   * This method leverages the ability to open a stream from GCS from a given offset. When the
   * underlying stream has fewer bytes buffered than the skip request, the stream is closed, and
   * a new stream is opened starting at the requested offset.
   *
   * @param n number of bytes to skip
   * @return the number of bytes skipped
   */
  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    mPos += n;
    if (mReadChannel == null) {
      openStream();
    } else {
      mReadChannel.seek(mPos);
    }
    return n;
  }

  /**
   * Opens a new stream at mPos.
   */
  private void openStream() throws IOException {
    try {
      mReadChannel = mClient.reader(BlobId.of(mBucketName, mKey));
      if (mReadChannel != null) {
        if (mPos > 0) {
          mReadChannel.seek(mPos);
        }
      } else {
        throw new IOException(String
            .format("Failed to open stream of %s in %s", mKey, mBucketName));
      }
    } catch (StorageException e) {
      throw new IOException(String
          .format("Failed to open stream of %s in %s", mKey, mBucketName), e);
    }
  }
}
