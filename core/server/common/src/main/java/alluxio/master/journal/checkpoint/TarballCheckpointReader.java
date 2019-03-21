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
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Reads a tarball-based checkpoint.
 */
public class TarballCheckpointReader {
  private final InputStream mStream;

  /**
   * @param in the checkpoint input stream to read from
   */
  public TarballCheckpointReader(CheckpointInputStream in) {
    // We may add new tarball-based checkpoint types in the future.
    Preconditions.checkState(in.getType() == CheckpointType.ROCKS, "Unexpected checkpoint type: %s",
        in.getType());
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
