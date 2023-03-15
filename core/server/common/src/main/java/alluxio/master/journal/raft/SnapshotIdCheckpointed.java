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

package alluxio.master.journal.raft;

import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.journal.checkpoint.CheckpointOutputStream;
import alluxio.master.journal.checkpoint.CheckpointType;
import alluxio.master.journal.checkpoint.Checkpointed;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Simple implementation to write and recover the snapshot ID when checkpointing.
 */
public class SnapshotIdCheckpointed implements Checkpointed {
  private long mId;

  private SnapshotIdCheckpointed(long id) {
    mId = id;
  }

  static SnapshotIdCheckpointed forWriting(long id) {
    return new SnapshotIdCheckpointed(id);
  }

  static SnapshotIdCheckpointed forReading() {
    return new SnapshotIdCheckpointed(-1);
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.SNAPSHOT_ID;
  }

  @Override
  public void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException {
    DataOutputStream dataStream = new CheckpointOutputStream(output, CheckpointType.LONG);
    dataStream.writeLong(mId);
  }

  @Override
  public void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    mId = input.readLong();
  }

  /**
   * @return the snapshot id
   */
  public long getId() {
    return mId;
  }
}
