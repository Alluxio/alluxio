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

package alluxio.master.journal.checkpoint;

import alluxio.util.FormatUtils;

import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.lz77support.Parameters;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;

/**
 * OutputStream to write checkpoint files efficiently.
 */
public class OptimizedCheckpointOutputStream extends OutputStream {
  public static final int BUFFER_SIZE = (int) FormatUtils.parseSpaceSize("4MB");

  private final OutputStream mOutputStream;

  /**
   * @param file where the checkpoint will be written
   * @throws IOException propagates wrapped output stream exceptions
   */
  public OptimizedCheckpointOutputStream(File file) throws IOException {
    Parameters options = BlockLZ4CompressorOutputStream.createParameterBuilder()
        .tunedForSpeed()
        .build();

    mOutputStream = new BlockLZ4CompressorOutputStream(
        new BufferedOutputStream(Files.newOutputStream(file.toPath()), BUFFER_SIZE),
        options);
  }

  @Override
  public void write(int b) throws IOException {
    mOutputStream.write(b);
  }

  @Override
  public void close() throws IOException {
    mOutputStream.close();
  }
}
