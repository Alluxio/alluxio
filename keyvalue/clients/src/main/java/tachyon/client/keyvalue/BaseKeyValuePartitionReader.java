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
import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.block.TachyonBlockStore;
import tachyon.exception.TachyonException;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.WorkerNetAddress;
import tachyon.util.io.BufferUtils;

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
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   * @throws IOException if a non-Tachyon exception occurs
   */
  BaseKeyValuePartitionReader(long blockId) throws TachyonException, IOException {
    mBlockId = blockId;
    BlockInfo info = TachyonBlockStore.get().getInfo(mBlockId);
    WorkerNetAddress workerAddr = info.getLocations().get(0).getWorkerAddress();
    mClient = new KeyValueWorkerClient(workerAddr, ClientContext.getConf());
    mClosed = false;
  }

  // This could be slow when value size is large, use with caution.
  @Override
  public byte[] get(byte[] key) throws IOException, TachyonException {
    ByteBuffer keyBuffer = ByteBuffer.wrap(key);
    ByteBuffer value = getInternal(keyBuffer);
    if (value == null) {
      return null;
    }
    return BufferUtils.newByteArrayFromByteBuffer(value);
  }

  @Override
  public ByteBuffer get(ByteBuffer key) throws IOException, TachyonException {
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
   * @throws TachyonException if a Tachyon error occurs
   */
  private ByteBuffer getInternal(ByteBuffer key) throws IOException, TachyonException {
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
     * @throws IOException if a non-Tachyon error happens when getting the first key-value pair
     * @throws TachyonException if a Tachyon error happens when getting the first key-value pair
     */
    public Iterator() throws IOException, TachyonException {
      mNextKey = nextKey(null);
    }

    @Override
    public boolean hasNext() {
      return mNextKey != null;
    }

    @Override
    public KeyValuePair next() throws IOException, TachyonException {
      KeyValuePair ret = new KeyValuePair(mNextKey, get(mNextKey));
      mNextKey = nextKey(mNextKey);
      return ret;
    }

    private ByteBuffer nextKey(ByteBuffer key) throws IOException, TachyonException {
      List<ByteBuffer> nextKeys = mClient.getNextKeys(mBlockId, key, 1);
      if (nextKeys.size() > 0) {
        return nextKeys.get(0);
      }
      return null;
    }
  }

  @Override
  public KeyValueIterator iterator() throws IOException, TachyonException {
    return new Iterator();
  }

  @Override
  public int size() throws IOException, TachyonException {
    return mClient.getSize(mBlockId);
  }
}
