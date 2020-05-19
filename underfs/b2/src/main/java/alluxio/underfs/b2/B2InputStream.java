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

import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.contentHandlers.B2ContentMemoryWriter;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2DownloadByNameRequest;
import com.backblaze.b2.util.B2ByteRange;
import com.google.common.base.Throwables;
import com.google.protobuf.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for reading a file from B2.
 */
@NotThreadSafe
public class B2InputStream extends InputStream {
  private static final Logger LOG = LoggerFactory.getLogger(B2InputStream.class);
  private final String mBucketName;
  private final String mKey;
  private final B2StorageClient mB2StorageClient;
  private long mOffset;
  private long mLength;
  private BufferedInputStream mBufferedInputStream;

  /**
   * Constructor for an input stream of an object in B2 using the B2-sdk implementation to read
   * data.
   *
   * @param bucketName the bucket the object resides in
   * @param key the path of the object to read
   * @param b2StorageClient the b2 client to use for operations
   * @param length the length of the stream in bytes
   */
  public B2InputStream(String bucketName, String key, B2StorageClient b2StorageClient, long length)
      throws ServiceException {
    this(bucketName, key, b2StorageClient, 0L, length);
  }

  /**
   * Constructor for an input stream of an object in B2 using the B2-sdk implementation to read
   * data.
   *
   * @param bucketName the bucket the object resides in
   * @param key the path of the object to read
   * @param b2StorageClient the b2 client to use for operations
   * @param offset the stream position to begin reading from
   * @param length the length of the stream in bytes
   */
  public B2InputStream(String bucketName, String key, B2StorageClient b2StorageClient, long offset,
      long length) throws ServiceException {
    mBucketName = bucketName;
    mKey = key;
    mB2StorageClient = b2StorageClient;
    mOffset = offset;
    mLength = length;

    try {
      final B2ContentMemoryWriter sink = getSink();
      if (mOffset > 0) {
        final B2DownloadByNameRequest request = B2DownloadByNameRequest.builder(mBucketName, mKey)
            .setRange(B2ByteRange.between(mOffset, mLength)).build();
        mB2StorageClient.downloadByName(request, sink);
      } else {
        final B2DownloadByNameRequest request =
            B2DownloadByNameRequest.builder(mBucketName, mKey).build();
        mB2StorageClient.downloadByName(request, sink);
      }
      mBufferedInputStream = new BufferedInputStream(new ByteArrayInputStream(sink.getBytes()));
    } catch (B2Exception e) {
      LOG.error(
          "Failed to open stream with remote storage. bucket name: [{}], key: [{}], offset: [{}]",
          mBucketName, mKey, mOffset);
      Throwables.propagateIfPossible(e, ServiceException.class);
    }
  }

  @Override
  public void close() throws IOException {
    mBufferedInputStream.close();
  }

  protected B2ContentMemoryWriter getSink() {
    return B2ContentMemoryWriter.build();
  }

  @Override
  public int read() throws IOException {
    int ret = mBufferedInputStream.read();
    if (ret != -1) {
      mOffset++;
    }
    return ret;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int ret = mBufferedInputStream.read(b, off, len);
    if (ret != -1) {
      mOffset += ret;
    }
    return ret;
  }

  /**
   * This method leverages the ability to open a stream from B2 from a given offset. When the
   * underlying stream has fewer bytes buffered than the skip request, the stream is closed, and
   * a new stream is opened starting at the requested offset.
   *
   * @param n number of bytes to skip
   * @return the number of bytes skipped
   */
  @Override
  public long skip(long n) throws IOException {
    if (mBufferedInputStream.available() >= n) {
      return mBufferedInputStream.skip(n);
    }
    // The number of bytes to skip is possibly large, open a new stream from B2.
    mBufferedInputStream.close();
    mOffset += n;
    try {
      final B2ContentMemoryWriter sink = getSink();
      final B2DownloadByNameRequest request = B2DownloadByNameRequest.builder(mBucketName, mKey)
          .setRange(B2ByteRange.between(mOffset, mLength)).build();
      mB2StorageClient.downloadByName(request, sink);
      mBufferedInputStream = new BufferedInputStream(new ByteArrayInputStream(sink.getBytes()));
    } catch (B2Exception e) {
      throw new IOException(e);
    }
    return n;
  }
}
