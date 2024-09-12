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

package alluxio.network.protocol.databuffer;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.ResourceExhaustedRuntimeException;
import alluxio.retry.RetryPolicy;
import alluxio.util.CleanerUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

/**
 * Direct buffer pool.
 */
public class NioDirectBufferPool {
  private static final Logger LOG = LoggerFactory.getLogger(NioDirectBufferPool.class);
  private static final TreeMap<Integer, LinkedList<ByteBuffer>> BUF_POOL = new TreeMap();
  private static final boolean POOLED_BUFFER_ENABLED =
      Configuration.getBoolean(PropertyKey.WORKER_POOLED_DIRECT_BUFFER_ENABLED);

  /**
   * @param length
   * @return buffer
   */
  public static synchronized ByteBuffer acquire(int length) {
    Map.Entry<Integer, LinkedList<ByteBuffer>> entry = BUF_POOL.ceilingEntry(length);
    if (entry == null || entry.getValue().size() == 0) {
      return ByteBuffer.allocateDirect(length);
    }
    ByteBuffer buffer = entry.getValue().pop();
    buffer.clear();
    // the buffer probably is larger than the amount of capacity being requested
    // need to set the limit explicitly
    buffer.limit(length);
    return buffer;
  }

  /**
   * @param length
   * @param policy the retry policy to use
   * @return buffer
   */
  public static synchronized ByteBuffer acquire(int length, RetryPolicy policy) {
    Error cause = null;
    while (policy.attempt()) {
      try {
        return acquire(length);
      } catch (OutOfMemoryError error) {
        cause = error;
      }
    }
    throw new ResourceExhaustedRuntimeException("Not enough direct memory allocated to buffer",
        cause, false);
  }

  /**
   * @param buffer
   */
  public static synchronized void release(ByteBuffer buffer) {
    if (!POOLED_BUFFER_ENABLED && buffer.isDirect()) {
      free(buffer);
    } else {
      LinkedList<ByteBuffer> bufList = BUF_POOL.get(buffer.capacity());
      if (bufList == null) {
        bufList = new LinkedList<>();
        BUF_POOL.put(buffer.capacity(), bufList);
      }
      bufList.push(buffer);
    }
  }

  /**
   * Forcibly free the direct buffer.
   *
   * @param buffer buffer
   */
  private static void free(ByteBuffer buffer) {
    if (CleanerUtils.UNMAP_SUPPORTED) {
      try {
        CleanerUtils.getCleaner().freeBuffer(buffer);
      } catch (IOException e) {
        LOG.info("Failed to free the buffer", e);
      }
    } else {
      LOG.trace(CleanerUtils.UNMAP_NOT_SUPPORTED_REASON);
    }
  }
}
