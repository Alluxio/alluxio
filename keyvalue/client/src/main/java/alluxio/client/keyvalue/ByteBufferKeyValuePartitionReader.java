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

import alluxio.exception.AlluxioException;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.ByteIOUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Reader that implements {@link KeyValuePartitionReader} to access a key-value file using random
 * access API.
 */
@NotThreadSafe
public final class ByteBufferKeyValuePartitionReader implements KeyValuePartitionReader {
  private static final Logger LOG =
      LoggerFactory.getLogger(ByteBufferKeyValuePartitionReader.class);

  private Index mIndex;
  private PayloadReader mPayloadReader;
  private ByteBuffer mBuf;
  private int mBufferLength;
  /** Whether this writer is closed. */
  private boolean mClosed;

  /**
   * Constructs {@link ByteBufferKeyValuePartitionReader}.
   *
   * @param fileBytes the byte buffer as underline storage to read from
   */
  public ByteBufferKeyValuePartitionReader(ByteBuffer fileBytes) {
    mBuf = Preconditions.checkNotNull(fileBytes);
    mBufferLength = mBuf.remaining();
    mIndex = createIndex();
    mPayloadReader = createPayloadReader();
    mClosed = false;
  }

  private Index createIndex() {
    int indexOffset = ByteIOUtils.readInt(mBuf, mBufferLength - 4);
    ByteBuffer indexBytes =
        BufferUtils.sliceByteBuffer(mBuf, indexOffset, mBufferLength - 4 - indexOffset);
    return LinearProbingIndex.loadFromByteArray(indexBytes);
  }

  private PayloadReader createPayloadReader() {
    return new BasePayloadReader(mBuf);
  }

  /**
   * {@inheritDoc}
   * <p>
   * This could be slow when value size is large, use this cautiously or {@link #get(ByteBuffer)}
   * which may avoid copying data.
   */
  @Override
  public byte[] get(byte[] key) throws IOException {
    ByteBuffer valueBuffer = get(ByteBuffer.wrap(key));
    if (valueBuffer == null) {
      return null;
    }
    return BufferUtils.newByteArrayFromByteBuffer(valueBuffer);
  }

  @Override
  public ByteBuffer get(ByteBuffer key) throws IOException {
    Preconditions.checkState(!mClosed);
    LOG.trace("get: key");
    return mIndex.get(key, mPayloadReader);
  }

  @Override
  public void close() {
    if (mClosed) {
      return;
    }
    mClosed = true;
  }

  @Override
  public KeyValueIterator iterator() {
    return new KeyValueIterator() {
      private Iterator<ByteBuffer> mKeyIterator = mIndex.keyIterator(mPayloadReader);

      @Override
      public boolean hasNext() {
        return mKeyIterator.hasNext();
      }

      @Override
      public KeyValuePair next() throws IOException, AlluxioException {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        ByteBuffer key = mKeyIterator.next();
        ByteBuffer value = get(key);
        return new KeyValuePair(key, value);
      }
    };
  }

  @Override
  public int size() throws IOException, AlluxioException {
    return mIndex.keyCount();
  }

  /**
   * @return the {@link Index} reconstructed from the byte buffer
   */
  public Index getIndex() {
    return mIndex;
  }

  /**
   * @return the {@link PayloadReader} for reading payloads from the byte buffer
   */
  public PayloadReader getPayloadReader() {
    return mPayloadReader;
  }
}
