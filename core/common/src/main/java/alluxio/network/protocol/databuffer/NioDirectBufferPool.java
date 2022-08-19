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
