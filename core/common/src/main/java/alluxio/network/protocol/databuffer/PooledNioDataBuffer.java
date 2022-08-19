package alluxio.network.protocol.databuffer;

import java.nio.ByteBuffer;

/**
 * The underlying byte buffer is a pooled byte buffer.
 */
public class PooledNioDataBuffer extends NioDataBuffer {

  /**
   * @param buffer The ByteBuffer representing the data
   * @param length The length of the ByteBuffer
   */
  public PooledNioDataBuffer(ByteBuffer buffer, long length) {
    super(buffer, length);
  }

  @Override
  public void release() {
    NioDirectBufferPool.release(mBuffer);
  }
}
