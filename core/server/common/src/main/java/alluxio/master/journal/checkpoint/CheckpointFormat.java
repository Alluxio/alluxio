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

import java.io.IOException;
import java.io.PrintStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Interface for checkpoint formats.
 *
 * Different components of Alluxio write checkpoints of their state using different formats.
 *
 * {@link CheckpointType} holds an association from checkpoint type to checkpoint format.
 */
public interface CheckpointFormat {
  /**
   * @param in a checkpoint input stream
   * @return a reader for reading from the checkpoint
   */
  CheckpointReader createReader(CheckpointInputStream in);

  /**
   * @param in a checkpoint input stream
   * @param out a print stream for writing a human-readable checkpoint representation
   */
  void parseToHumanReadable(CheckpointInputStream in, PrintStream out) throws IOException;

  /**
   * Interface for checkpoint readers.
   */
  @NotThreadSafe
  interface CheckpointReader {}
}
