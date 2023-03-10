package alluxio.worker.block;

import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.StoreBlockReader;
import alluxio.worker.block.meta.BlockMeta;
import java.io.IOException;
import java.nio.channels.FileChannel;
import com.google.inject.Inject;

/**
 * Factory for tiered block reader.
 */
public class TieredBlockReaderFactory implements BlockReaderFactory {

  @Override
  public BlockReader createBlockReader(long sessionId, BlockMeta blockMeta, long offset)
      throws IOException {
    BlockReader reader = new StoreBlockReader(sessionId, blockMeta);
    ((FileChannel) reader.getChannel()).position(offset);
    return reader;
  }
}
