package tachyon.worker.netty;

/**
 * How a read response will transfer block data over the network. There is a difference in speed and
 * memory consumption between the two. {@link #MAPPED} is the default since at larger sizes it out
 * performs {@link #TRANSFER}
 */
public enum FileTransferType {
  /**
   * Uses a {@link java.nio.MappedByteBuffer} to transfer data over the network
   */
  MAPPED,

  /**
   * Uses
   * {@link java.nio.channels.FileChannel#transferTo(long, long, java.nio.channels.WritableByteChannel)}
   * to transfer data over the network
   */
  TRANSFER
}
