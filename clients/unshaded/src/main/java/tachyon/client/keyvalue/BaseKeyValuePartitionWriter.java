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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.client.file.AbstractCountingOutStream;
import tachyon.util.io.ByteIOUtils;

/**
 * Writer that implements {@link KeyValuePartitionWriter} using Tachyon file stream interface to
 * generate a key-value file.
 *
 * TODO(binfan): describe the key-value file format
 * <p>
 * This class is not thread-safe.
 */
public final class BaseKeyValuePartitionWriter implements KeyValuePartitionWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** handle to write to the underlying file */
  private final AbstractCountingOutStream mFileOutStream;
  /** number of key-value pairs added */
  private long mKeyCount = 0;
  /** key-value index */
  private Index mIndex;
  /** key-value payload */
  private PayloadWriter mPayloadWriter;
  /** whether this writer is closed */
  private boolean mClosed;

  /**
   * @param fileOutStream output stream to store the key-value file
   */
  public BaseKeyValuePartitionWriter(AbstractCountingOutStream fileOutStream) {
    mFileOutStream = Preconditions.checkNotNull(fileOutStream);
    // TODO(binfan): write a header in the file

    mPayloadWriter = new OutStreamPayloadWriter(mFileOutStream);
    // Use linear probing impl of index for now
    mIndex = LinearProbingIndex.createEmptyIndex();
    mClosed = false;
  }

  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);
    Preconditions.checkState(!mClosed);
    mIndex.put(key, value, mPayloadWriter);
    mKeyCount ++;
  }

  @Override
  public void close() throws IOException {
    Preconditions.checkState(!mClosed);
    build();
    mFileOutStream.close();
    mClosed = true;
  }

  @Override
  public boolean isFull() {
    // TODO(binfan): make this configurable.
    if (byteCount() > Constants.GB) {
      return true;
    }
    return false;
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
