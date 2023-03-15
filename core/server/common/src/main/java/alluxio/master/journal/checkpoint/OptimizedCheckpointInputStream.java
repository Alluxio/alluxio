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

import net.jpountz.lz4.LZ4FrameInputStream;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.DigestInputStream;
import java.security.MessageDigest;

/**
 * InputStream to read checkpoint files efficiently.
 */
public class OptimizedCheckpointInputStream extends CheckpointInputStream {

  /**
   * @param file where the checkpoint will be read from
   * @param digest that verifies the file has not been corrupted
   * @throws IOException propagates wrapped input stream exceptions
   */
  public OptimizedCheckpointInputStream(File file, MessageDigest digest) throws IOException {
    super(new DigestInputStream(new LZ4FrameInputStream(
        new BufferedInputStream(Files.newInputStream(file.toPath()),
            OptimizedCheckpointOutputStream.BUFFER_SIZE)), digest));
  }
}
