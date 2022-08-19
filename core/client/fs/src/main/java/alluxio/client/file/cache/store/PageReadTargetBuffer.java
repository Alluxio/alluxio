package alluxio.client.file.cache.store;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 *
 */
public interface PageReadTargetBuffer{

  /**
   * @return
   */
  boolean hasByteArray();

  /**
   * @return
   */
  byte[] byteArray();

  /**
   *
   * @return
   */
  boolean hasByteBuffer();

  /**
   *
   * @return
   */
  ByteBuffer byteBuffer();

  /**
   *
   * @return
   */
  long offset();

  /**
   *
   * @return
   */
  WritableByteChannel byteChannel();

  /**
   *
   * @return
   */
  long remaining();
}
