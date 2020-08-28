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

import java.io.IOException;
import java.io.PrintStream;

/**
 * Format for checkpoints containing a single long.
 */
public class LongCheckpointFormat implements CheckpointFormat {
  @Override
  public LongCheckpointReader createReader(CheckpointInputStream in) {
    return new LongCheckpointReader(in);
  }

  @Override
  public void parseToHumanReadable(CheckpointInputStream in, PrintStream out) throws IOException {
    LongCheckpointReader reader = createReader(in);
    out.println(reader.getLong());
  }

  /**
   * Reads a checkpoint containing a single long.
   */
  public static class LongCheckpointReader implements CheckpointReader {
    private final CheckpointInputStream mStream;

    /**
     * @param in the checkpoint stream to read from
     */
    public LongCheckpointReader(CheckpointInputStream in) {
      Preconditions.checkState(in.getType() == CheckpointType.LONG,
          "Unexpected checkpoint type: %s", in.getType());
      mStream = in;
    }

    /**
     * @return the long in the checkpoint
     */
    public Long getLong() throws IOException {
      return mStream.readLong();
    }
  }
}
