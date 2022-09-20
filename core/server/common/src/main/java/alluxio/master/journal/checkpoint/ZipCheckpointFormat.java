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

import java.io.PrintStream;

/**
 * Format for checkpoints written as tarballs.
 */
public class ZipCheckpointFormat implements CheckpointFormat {
  @Override
  public ZipCheckpointReader createReader(CheckpointInputStream in) {
    return new ZipCheckpointReader(in);
  }

  @Override
  public void parseToHumanReadable(CheckpointInputStream in, PrintStream out) {
    out.println("No human-readable string representation available. Use bin/alluxio readJournal "
        + "to inspect the checkpoint");
  }

  /**
   * Reads a tarball-based checkpoint.
   */
  public static class ZipCheckpointReader implements CheckpointReader {

    /**
     * @param in the checkpoint input stream to read from
     */
    public ZipCheckpointReader(CheckpointInputStream in) {
      // We may add new tarball-based checkpoint types in the future.
      Preconditions.checkState(in.getType() == CheckpointType.ROCKS_ZIP,
          "Unexpected checkpoint type: %s", in.getType());
    }
  }
}
