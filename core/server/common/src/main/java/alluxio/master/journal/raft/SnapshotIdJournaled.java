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

import alluxio.master.journal.SingleEntryJournaled;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.proto.journal.Journal;

/**
 * Simple implementation to write and recover the snapshot ID when checkpointing. The snapshot ID
 * is a long that represents the sequence number of the last entry that was processed by the
 * journal. The snapshot ID will be inserted into and retrieved through the
 * {@link Journal.JournalEntry.Builder#setSequenceNumber(long)} and
 * {@link Journal.JournalEntry.Builder#getSequenceNumber()} methods, respectively.
 */
public class SnapshotIdJournaled extends SingleEntryJournaled {
  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.SNAPSHOT_ID;
  }
}
