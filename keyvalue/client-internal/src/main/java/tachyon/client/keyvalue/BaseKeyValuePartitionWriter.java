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

package tachyon.client.keyvalue;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.OutStreamBase;
import tachyon.util.io.ByteIOUtils;

/**
 * Writer that implements {@link KeyValuePartitionWriter} using the Tachyon file stream interface to
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

  /** handle to write to the underlying file */
  private final OutStreamBase mFileOutStream;
  /** number of key-value pairs added */
  private long mKeyCount = 0;
  /** key-value index */
  private Index mIndex;
  /** key-value payload */
  private PayloadWriter mPayloadWriter;
  /** whether this writer is closed */
  private boolean mClosed;
  /** whether this writer is canceled */
  private boolean mCanceled;
  /** maximum size of this partition in bytes */
  private long mMaxSizeBytes;

  /**
   * Constructs a {@link BaseKeyValuePartitionWriter} given an output stream.
   *
   * @param fileOutStream output stream to store the key-value file
   */
  BaseKeyValuePartitionWriter(OutStreamBase fileOutStream) {
    mFileOutStream = Preconditions.checkNotNull(fileOutStream);
    // TODO(binfan): write a header in the file

    mPayloadWriter = new BasePayloadWriter(mFileOutStream);
    mIndex = LinearProbingIndex.createEmptyIndex();
    mClosed = false;
    mCanceled = false;
    mMaxSizeBytes = ClientContext.getConf().getBytes(Constants.KEY_VALUE_PARTITION_SIZE_BYTES_MAX);
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
    mKeyCount ++;
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
  public long keyCount() {
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
