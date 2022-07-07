package alluxio.worker.block;

import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.StoreBlockWriter;
import alluxio.worker.block.meta.TempBlockMeta;

import java.io.IOException;

/**
 * Factory for tiered block writer.
 */
public class TieredBlockWriterFactory implements BlockWriterFactory {
  @Override
  public BlockWriter createBlockWriter(TempBlockMeta tempBlockMeta) throws IOException {
    return new StoreBlockWriter(tempBlockMeta);
  }
}
