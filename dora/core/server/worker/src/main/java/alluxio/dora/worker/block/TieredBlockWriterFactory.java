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

package alluxio.dora.worker.block;

import alluxio.dora.worker.block.io.BlockWriter;
import alluxio.dora.worker.block.io.StoreBlockWriter;
import alluxio.dora.worker.block.meta.TempBlockMeta;

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
