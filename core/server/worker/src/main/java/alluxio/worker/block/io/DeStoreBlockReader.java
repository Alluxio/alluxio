package alluxio.worker.block.io;

import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * An reader class with metrics.
 */
public class DeStoreBlockReader extends BlockReader {
  private final BlockReader mDeBlockReader;

  /**
   * A decorator of BlockReader.
   * @param deblockReader block reader
   */
  public DeStoreBlockReader(BlockReader deblockReader) {
    mDeBlockReader = deblockReader;
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    return mDeBlockReader.read(offset, length);
  }

  @Override
  public long getLength() {
    return mDeBlockReader.getLength();
  }

  @Override
  public ReadableByteChannel getChannel() {
    return mDeBlockReader.getChannel();
  }

  @Override
  public int transferTo(ByteBuf buf) throws IOException {
    int bytesReadFromCache = mDeBlockReader.transferTo(buf);
    MetricsSystem.counter(MetricKey.WORKER_BYTES_READ_CACHE.getName()).inc(bytesReadFromCache);
    return bytesReadFromCache;
  }

  @Override
  public boolean isClosed() {
    return mDeBlockReader.isClosed();
  }

  @Override
  public String getLocation() {
    return mDeBlockReader.getLocation();
  }

  @Override
  public String toString() {
    return mDeBlockReader.toString();
  }
}
