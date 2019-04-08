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

import alluxio.proto.meta.InodeMeta;
import alluxio.proto.meta.InodeMeta.Inode;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * Reads inode proto checkpoints.
 */
public class InodeProtosCheckpointReader {
  private final InputStream mStream;

  /**
   * @param in a checkpoint stream to read from
   */
  public InodeProtosCheckpointReader(CheckpointInputStream in) {
    Preconditions.checkState(in.getType() == CheckpointType.INODE_PROTOS);
    mStream = in;
  }

  /**
   * @return the next inode proto in the checkpoint, or empty if we've reached the end
   */
  public Optional<Inode> read() throws IOException {
    return Optional.ofNullable(InodeMeta.Inode.parseDelimitedFrom(mStream));
  }
}
