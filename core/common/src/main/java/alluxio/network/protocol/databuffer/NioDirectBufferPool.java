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

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

/**
 * Direct buffer pool.
 */
public class NioDirectBufferPool {
  private static final TreeMap<Integer, LinkedList<ByteBuffer>> BUF_POOL = new TreeMap();

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
   * @param buffer
   */
  public static synchronized void release(ByteBuffer buffer) {
    LinkedList<ByteBuffer> bufList = BUF_POOL.get(buffer.capacity());
    if (bufList == null) {
      bufList = new LinkedList<>();
      BUF_POOL.put(buffer.capacity(), bufList);
    }
    bufList.push(buffer);
  }
}
