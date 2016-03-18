/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.keyvalue;

import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.exception.AlluxioException;
import alluxio.util.io.BufferUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Default implementation of {@link KeyValuePartitionReader} to talk to a remote key-value worker to
 * get the value of a given key.
 */
@NotThreadSafe
final class BaseKeyValuePartitionReader implements KeyValuePartitionReader {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private KeyValueWorkerClient mClient;
  private long mBlockId;
  private boolean mClosed;

  // TODO(binfan): take parition id as input
  /**
   * Constructs {@link BaseKeyValuePartitionReader} given a block id.
   *
   * @param blockId blockId of the key-value file to read from
   * @throws AlluxioException if an unexpected Alluxio exception is thrown
   * @throws IOException if a non-Alluxio exception occurs
   */
  BaseKeyValuePartitionReader(long blockId) throws AlluxioException, IOException {
    mBlockId = blockId;
    BlockInfo info = AlluxioBlockStore.get().getInfo(mBlockId);
    WorkerNetAddress workerAddr = info.getLocations().get(0).getWorkerAddress();
    mClient = new KeyValueWorkerClient(workerAddr, ClientContext.getConf());
    mClosed = false;
  }

  // This could be slow when value size is large, use with caution.
  @Override
  public byte[] get(byte[] key) throws IOException, AlluxioException {
    ByteBuffer keyBuffer = ByteBuffer.wrap(key);
    ByteBuffer value = getInternal(keyBuffer);
    if (value == null) {
      return null;
    }
    return BufferUtils.newByteArrayFromByteBuffer(value);
  }

  @Override
  public ByteBuffer get(ByteBuffer key) throws IOException, AlluxioException {
    return getInternal(key);
  }

  @Override
  public void close() {
    if (mClosed) {
      return;
    }
    mClient.close();
    mClosed = true;
  }

  /**
   * Returns the value in {@link ByteBuffer} in this partition, or null if not found.
   *
   * @param key the key to lookup
   * @return the value of this key
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  private ByteBuffer getInternal(ByteBuffer key) throws IOException, AlluxioException {
    Preconditions.checkState(!mClosed, "Can not query a reader closed");
    ByteBuffer value = mClient.get(mBlockId, key);
    if (value.remaining() == 0) {
      return null;
    }
    return value;
  }

  private class Iterator implements KeyValueIterator {
    private ByteBuffer mNextKey;

    /**
     * Gets the first key-value pair and constructs a new key-value partition iterator.
     *
     * @throws IOException if a non-Alluxio error happens when getting the first key-value pair
     * @throws AlluxioException if an Alluxio error happens when getting the first key-value pair
     */
    public Iterator() throws IOException, AlluxioException {
      mNextKey = nextKey(null);
    }

    @Override
    public boolean hasNext() {
      return mNextKey != null;
    }

    @Override
    public KeyValuePair next() throws IOException, AlluxioException {
      KeyValuePair ret = new KeyValuePair(mNextKey, get(mNextKey));
      mNextKey = nextKey(mNextKey);
      return ret;
    }

    private ByteBuffer nextKey(ByteBuffer key) throws IOException, AlluxioException {
      List<ByteBuffer> nextKeys = mClient.getNextKeys(mBlockId, key, 1);
      if (!nextKeys.isEmpty()) {
        return nextKeys.get(0);
      }
      return null;
    }
  }

  @Override
  public KeyValueIterator iterator() throws IOException, AlluxioException {
    return new Iterator();
  }

  @Override
  public int size() throws IOException, AlluxioException {
    return mClient.getSize(mBlockId);
  }
}
