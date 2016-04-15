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

package alluxio.worker.keyvalue;

import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.RpcUtils.RpcCallableThrowsIOException;
import alluxio.client.keyvalue.ByteBufferKeyValuePartitionReader;
import alluxio.client.keyvalue.Index;
import alluxio.client.keyvalue.PayloadReader;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.KeyValueWorkerClientService;
import alluxio.thrift.ThriftIOException;
import alluxio.util.io.BufferUtils;
import alluxio.worker.block.BlockWorker;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * RPC service handler on worker side to read a local key-value block.
 */
// TODO(binfan): move logic outside and make this a simple wrapper.
@ThreadSafe
public final class KeyValueWorkerClientServiceHandler implements KeyValueWorkerClientService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** A cache pool for ByteBufferKeyValuePartitionReader, map from blockId. */
  private final ReaderCachePool mReaderPool;

  /**
   * @param blockWorker the {@link BlockWorker}
   */
  public KeyValueWorkerClientServiceHandler(BlockWorker blockWorker) {
    mReaderPool = new ReaderCachePool(10, blockWorker);
  }

  @Override
  public long getServiceVersion() {
    return Constants.KEY_VALUE_WORKER_SERVICE_VERSION;
  }

  /**
   * Gets the value for {@code key} in the given block, or null if key is not found.
   *
   * @param blockId block Id
   * @param key key to fetch
   * @return value or null if not found
   * @throws AlluxioTException if an exception in Alluxio occurs
   * @throws ThriftIOException if a non-Alluxio related exception occurs
   */
  @Override
  public ByteBuffer get(final long blockId, final ByteBuffer key)
      throws AlluxioTException, ThriftIOException {
    return RpcUtils.call(new RpcCallableThrowsIOException<ByteBuffer>() {
      @Override
      public ByteBuffer call() throws AlluxioException, IOException {
        ByteBuffer value = getInternal(blockId, key);
        if (value == null) {
          return ByteBuffer.allocate(0);
        }
        return copyAsNonDirectBuffer(value);
      }
    });
  }

  private ByteBuffer copyAsNonDirectBuffer(ByteBuffer directBuffer) {
    // Thrift assumes the ByteBuffer returned has array() method, which is not true if the
    // ByteBuffer is direct. We make a non-direct copy of the ByteBuffer to return.
    return BufferUtils.cloneByteBuffer(directBuffer);
  }

  /**
   * Internal logic to get value from the given block.
   *
   * @param blockId Block Id
   * @param keyBuffer bytes of key
   * @return key found in the key-value block or null if not found
   * @throws IOException if read operation failed
   * @throws BlockDoesNotExistException if the worker is not serving this block
   */
  private ByteBuffer getInternal(long blockId, ByteBuffer keyBuffer)
      throws BlockDoesNotExistException, IOException {
    try {
      return mReaderPool.getReader(blockId).get(keyBuffer);
    } catch (InvalidWorkerStateException e) {
      // We shall never reach here
      LOG.error("Reaching invalid state to get a key", e);
    }
    return null;
  }

  @Override
  public List<ByteBuffer> getNextKeys(final long blockId, final ByteBuffer key, final int numKeys)
      throws AlluxioTException, ThriftIOException {
    return RpcUtils.call(new RpcCallableThrowsIOException<List<ByteBuffer>>() {
      @Override
      public List<ByteBuffer> call() throws AlluxioException, IOException {
        try {
          ByteBufferKeyValuePartitionReader reader = mReaderPool.getReader(blockId);
          Index index = reader.getIndex();
          PayloadReader payloadReader = reader.getPayloadReader();

          List<ByteBuffer> ret = Lists.newArrayListWithExpectedSize(numKeys);
          ByteBuffer currentKey = key;
          for (int i = 0; i < numKeys; i++) {
            ByteBuffer nextKey = index.nextKey(currentKey, payloadReader);
            if (nextKey == null) {
              break;
            }
            ret.add(copyAsNonDirectBuffer(nextKey));
            currentKey = nextKey;
          }
          return ret;
        } catch (InvalidWorkerStateException e) {
          // We shall never reach here
          LOG.error("Reaching invalid state to get all keys", e);
        }
        return Collections.emptyList();
      }
    });
  }

  // TODO(cc): Try to remove the duplicated try-catch logic in other methods like getNextKeys.
  @Override
  public int getSize(final long blockId) throws AlluxioTException, ThriftIOException {
    return RpcUtils.call(new RpcCallableThrowsIOException<Integer>() {
      @Override
      public Integer call() throws AlluxioException, IOException {
        try {
          return mReaderPool.getReader(blockId).size();
        } catch (InvalidWorkerStateException e) {
          // We shall never reach here
          LOG.error("Reaching invalid state to get size", e);
        }
        return 0;
      }
    });
  }
}
