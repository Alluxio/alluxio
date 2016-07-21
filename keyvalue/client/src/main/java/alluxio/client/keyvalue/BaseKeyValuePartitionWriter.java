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

package alluxio.client.keyvalue;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.AbstractOutStream;
import alluxio.util.io.ByteIOUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Writer that implements {@link KeyValuePartitionWriter} using the Alluxio file stream interface to
 * generate a single-block key-value file.
 * <p>
 * A partition file consists of:
 * <ul>
 *   <li>A payload buffer which is an array of (key,value) pairs;</li>
 *   <li>A index which is a hash table maps each key to the offset in bytes into the payload
 *   buffer;</li>
 *   <li>A 4-bytes pointer in the end indicating the offset of the index.</li>
 * </ul>
 *
 */
@NotThreadSafe
final class BaseKeyValuePartitionWriter implements KeyValuePartitionWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Handle to write to the underlying file. */
  private final AbstractOutStream mFileOutStream;
  /** Number of key-value pairs added. */
  private int mKeyCount = 0;
  /** Key-value index. */
  private Index mIndex;
  /** Key-value payload. */
  private PayloadWriter mPayloadWriter;
  /** Whether this writer is closed. */
  private boolean mClosed;
  /** Whether this writer is canceled. */
  private boolean mCanceled;
  /** Maximum size of this partition in bytes. */
  private long mMaxSizeBytes;

  /**
   * Constructs a {@link BaseKeyValuePartitionWriter} given an output stream.
   *
   * @param fileOutStream output stream to store the key-value file
   */
  BaseKeyValuePartitionWriter(AbstractOutStream fileOutStream) {
    mFileOutStream = Preconditions.checkNotNull(fileOutStream);
    // TODO(binfan): write a header in the file

    mPayloadWriter = new BasePayloadWriter(mFileOutStream);
    mIndex = LinearProbingIndex.createEmptyIndex();
    mClosed = false;
    mCanceled = false;
    mMaxSizeBytes = Configuration.getBytes(PropertyKey.KEY_VALUE_PARTITION_SIZE_BYTES_MAX);
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    if (mCanceled) {
      mFileOutStream.cancel();
    } else {
      build();
      mFileOutStream.close();
    }
    mClosed = true;
  }

  @Override
  public void cancel() throws IOException {
    mCanceled = true;
    close();
  }

  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);
    Preconditions.checkArgument(key.length > 0, "Cannot put an empty key");
    Preconditions.checkArgument(value.length > 0, "Cannot put an empty value");
    Preconditions.checkState(!mClosed);
    mIndex.put(key, value, mPayloadWriter);
    mKeyCount++;
  }

  @Override
  public boolean canPut(byte[] key, byte[] value) {
    // See BasePayloadWriter.insert()
    // TODO(binfan): also take into account the potential index size change
    return byteCount() + key.length + value.length
        + Constants.BYTES_IN_INTEGER * 2 <= mMaxSizeBytes;
  }

  /**
   * @return number of keys
   */
  @Override
  public int keyCount() {
    return mKeyCount;
  }

  /**
   * @return number of bytes estimated
   */
  public long byteCount() {
    Preconditions.checkState(!mClosed);
    // last pointer to index
    return mFileOutStream.getBytesWritten() + mIndex.byteCount() + Integer.SIZE / Byte.SIZE;
  }

  private void build() throws IOException {
    Preconditions.checkState(!mClosed);
    mFileOutStream.flush();
    int indexOffset = mFileOutStream.getBytesWritten();
    mFileOutStream.write(mIndex.getBytes());
    ByteIOUtils.writeInt(mFileOutStream, indexOffset);
  }
}
