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

package alluxio.underfs.oss;

import alluxio.Configuration;
import alluxio.PropertyKey;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for reading a file from OSS. This input stream returns 0 when calling read with an empty
 * buffer.
 */
@NotThreadSafe
public class OSSInputStream extends InputStream {

  /** Bucket name of the Alluxio OSS bucket. */
  private final String mBucketName;

  /** Has the stream been closed. */
  private boolean mClosed;

  /** Key of the file in OSS to read. */
  private final String mKey;

  /** The OSS client for OSS operations. */
  private final OSSClient mOssClient;

  /** The underlying input stream. */
  private BufferedInputStream mInputStream;
  /** The current position of the stream. */
  private long mPos;

  /**
   * Creates a new instance of {@link OSSInputStream}.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the client for OSS
   * @throws IOException if an I/O error occurs
   */
  OSSInputStream(String bucketName, String key, OSSClient client) throws IOException {
    this(bucketName, key, client, 0L);
  }

  /**
   * Creates a new instance of {@link OSSInputStream}.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the client for OSS
   * @param position the position to begin reading from
   * @throws IOException if an I/O error occurs
   */
  OSSInputStream(String bucketName, String key, OSSClient client, long position)
      throws IOException {
    mBucketName = bucketName;
    mKey = key;
    mOssClient = client;
    mPos = position;
  }

  @Override
  public void close() throws IOException {
    if (!mClosed) {
      closeStream();
    }
    mClosed = true;
  }

  @Override
  public int read() throws IOException {
    if (mInputStream == null) {
      openStream();
    }
    int value = mInputStream.read();
    if (value != -1) { // valid data read
      mPos++;
      if (mPos % getBlockSize() == 0) {
        closeStream();
      }
    }
    return value;
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
    if (mInputStream == null) {
      openStream();
    }
    int read = mInputStream.read(b, off, len);
    if (read != -1) {
      mPos += read;
      if (mPos % getBlockSize() == 0) {
        closeStream();
      }
    }
    return read;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    closeStream();
    mPos += n;
    openStream();
    return n;
  }

  /**
   * Opens a new stream at mPos if the wrapped stream mStream is null.
   */
  private void openStream() throws IOException {
    if (mClosed) {
      throw new IOException("Stream closed");
    }
    if (mInputStream != null) { // stream is already open
      return;
    }
    GetObjectRequest req = new GetObjectRequest(mBucketName, mKey);
    final long blockSize = getBlockSize();
    final long endPos = mPos + blockSize - (mPos % blockSize);
    req.setRange(mPos, endPos);
    OSSObject ossObject = mOssClient.getObject(req);
    mInputStream = new BufferedInputStream(ossObject.getObjectContent());
  }

  /**
   * Closes the current stream.
   */
  private void closeStream() throws IOException {
    if (mInputStream == null) {
      return;
    }
    mInputStream.close();
    mInputStream = null;
  }

  /**
   * Block size for reading an object in chunks.
   *
   * @return block size in bytes
   */
  private long getBlockSize() {
    return Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }
}
