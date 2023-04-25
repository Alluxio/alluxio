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

package alluxio.master.file.meta;

import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.journal.checkpoint.CheckpointOutputStream;
import alluxio.master.journal.checkpoint.CheckpointType;
import alluxio.master.journal.checkpoint.Checkpointed;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.LongAdder;

/**
 * A checkpointed long adder.
 */
public final class InodeCounter extends LongAdder implements Checkpointed {
  private static final long serialVersionUID = 0;

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.INODE_COUNTER;
  }

  @Override
  public void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException {
    CheckpointOutputStream stream = new CheckpointOutputStream(output, CheckpointType.LONG);
    stream.writeLong(longValue());
    stream.flush();
  }

  @Override
  public void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    Preconditions.checkState(input.getType() == CheckpointType.LONG,
        "Unexpected checkpoint type: %s", input.getType());
    reset();
    add(input.readLong());
  }
}
