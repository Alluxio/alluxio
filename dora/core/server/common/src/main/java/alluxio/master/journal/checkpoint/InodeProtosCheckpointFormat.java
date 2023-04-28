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
import com.google.common.base.Strings;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Optional;

/**
 * Reads inode proto checkpoints.
 */
public class InodeProtosCheckpointFormat implements CheckpointFormat {
  private static final String ENTRY_SEPARATOR = Strings.repeat("-", 80);

  @Override
  public InodeProtosCheckpointReader createReader(CheckpointInputStream in) {
    return new InodeProtosCheckpointReader(in);
  }

  @Override
  public void parseToHumanReadable(CheckpointInputStream in, PrintStream out) throws IOException {
    InodeProtosCheckpointReader reader = createReader(in);
    Optional<InodeMeta.Inode> entry;
    while ((entry = reader.read()).isPresent()) {
      out.println(ENTRY_SEPARATOR);
      out.println(entry.get());
    }
  }

  private static class InodeProtosCheckpointReader implements CheckpointReader {
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
}
