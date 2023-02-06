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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;

/**
 * Base class for Alluxio classes which can be written to and read from metadata checkpoints.
 */
public interface Checkpointed {
  /**
   * @return a name for this checkpointed class
   */
  CheckpointName getCheckpointName();

  default void writeToCheckpoint(File directory) throws IOException, InterruptedException {
    File file = new File(directory, getCheckpointName().toString());
    try (FileOutputStream outputStream = new FileOutputStream(file)) {
      writeToCheckpoint(outputStream);
    }
  }

  /**
   * Writes a checkpoint of all state to the given output stream.
   *
   * Implementations should make an effort to throw {@link InterruptedException} if they get
   * interrupted while running.
   *
   * @param output the output stream to write to
   */
  void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException;

  default void restoreFromCheckpoint(File directory) throws IOException {
    File file = new File(directory, getCheckpointName().toString());
    try (CheckpointInputStream is = new CheckpointInputStream(Files.newInputStream(file.toPath()))) {
      restoreFromCheckpoint(is);
    }
  }

  /**
   * Restores state from a checkpoint.
   *
   * @param input an input stream with checkpoint data
   */
  void restoreFromCheckpoint(CheckpointInputStream input) throws IOException;
}
