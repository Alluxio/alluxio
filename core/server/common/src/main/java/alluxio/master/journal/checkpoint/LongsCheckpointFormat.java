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

import com.google.common.base.Preconditions;

import java.io.EOFException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Optional;

/**
 * Format for checkpoints consisting of a list of longs written by a DataOutputStream.
 */
public class LongsCheckpointFormat implements CheckpointFormat {
  @Override
  public LongsCheckpointReader createReader(CheckpointInputStream in) {
    return new LongsCheckpointReader(in);
  }

  @Override
  public void parseToHumanReadable(CheckpointInputStream in, PrintStream out) throws IOException {
    LongsCheckpointReader reader = createReader(in);
    Optional<Long> longOpt;
    while ((longOpt = reader.nextLong()).isPresent()) {
      out.printf("%d%n", longOpt.get());
    }
  }

  /**
   * Reads a checkpoint of longs.
   */
  public static class LongsCheckpointReader implements CheckpointReader {
    private final CheckpointInputStream mStream;

    /**
     * @param in the checkpoint stream to read from
     */
    public LongsCheckpointReader(CheckpointInputStream in) {
      Preconditions.checkState(in.getType() == CheckpointType.LONGS,
          "Unexpected checkpoint type: %s", in.getType());
      mStream = in;
    }

    /**
     * @return the next long in the checkpoint, or empty if the reader has reached the end of the
     * checkpoint
     */
    public Optional<Long> nextLong() throws IOException {
      try {
        return Optional.of(mStream.readLong());
      } catch (EOFException e) {
        return Optional.empty();
      }
    }
  }
}
