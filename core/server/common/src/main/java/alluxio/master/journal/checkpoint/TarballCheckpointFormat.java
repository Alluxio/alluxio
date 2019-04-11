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

import alluxio.util.TarUtils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Format for checkpoints written as tarballs.
 */
public class TarballCheckpointFormat implements CheckpointFormat {
  @Override
  public TarballCheckpointReader createReader(CheckpointInputStream in) {
    return new TarballCheckpointReader(in);
  }

  @Override
  public void parseToHumanReadable(CheckpointInputStream in, PrintStream out) throws IOException {
    out.println("No human-readable string representation available. Use bin/alluxio readJournal "
        + "to inspect the checkpoint");
  }

  /**
   * Reads a tarball-based checkpoint.
   */
  public static class TarballCheckpointReader implements CheckpointReader {
    private final InputStream mStream;

    /**
     * @param in the checkpoint input stream to read from
     */
    public TarballCheckpointReader(CheckpointInputStream in) {
      // We may add new tarball-based checkpoint types in the future.
      Preconditions.checkState(in.getType() == CheckpointType.ROCKS,
          "Unexpected checkpoint type: %s", in.getType());
      mStream = in;
    }

    /**
     * Unpacks the tarball data to the given path. Parent directories are created as needed.
     *
     * @param path the path to unpack to
     */
    public void unpackToDirectory(Path path) throws IOException {
      Files.createDirectories(path);
      TarUtils.readTarGz(path, mStream);
    }
  }
}
