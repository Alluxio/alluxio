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

import alluxio.exception.status.UnavailableException;
import alluxio.master.block.BlockId;
import alluxio.master.block.ContainerIdGenerable;
import alluxio.master.file.state.DirectoryId;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;
import alluxio.proto.journal.File.InodeDirectoryIdGeneratorEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.CloseableIterator;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Inode id management for directory inodes. Keep track of a block container id, along with a block
 * sequence number. If the block sequence number reaches the limit, a new block container id is
 * retrieved.
 */
@ThreadSafe
public class InodeDirectoryIdGenerator implements Journaled {
  private final ContainerIdGenerable mContainerIdGenerator;

  private final DirectoryId mNextDirectoryId;
  private boolean mInitialized = false;

  /**
   * @param containerIdGenerator the container id generator to use
   */
  public InodeDirectoryIdGenerator(ContainerIdGenerable containerIdGenerator) {
    mContainerIdGenerator =
        Preconditions.checkNotNull(containerIdGenerator, "containerIdGenerator");
    mNextDirectoryId = new DirectoryId();
  }

  /**
   * Returns the next directory id, and journals the state.
   *
   * @return the next directory id
   */
  synchronized long getNewDirectoryId(JournalContext context) throws UnavailableException {
    initialize(context);
    long containerId = mNextDirectoryId.getContainerId();
    long sequenceNumber = mNextDirectoryId.getSequenceNumber();
    long directoryId = BlockId.createBlockId(containerId, sequenceNumber);
    if (sequenceNumber == BlockId.getMaxSequenceNumber()) {
      // No more ids in this container. Get a new container for the next id.
      containerId = mContainerIdGenerator.getNewContainerId();
      sequenceNumber = 0;
    } else {
      sequenceNumber++;
    }
    applyAndJournal(context, toEntry(containerId, sequenceNumber));
    return directoryId;
  }

  private void initialize(JournalContext context) throws UnavailableException {
    if (!mInitialized) {
      applyAndJournal(context, toEntry(mContainerIdGenerator.getNewContainerId(), 0));
      mInitialized = true;
    }
  }

  /**
   * @param containerId a container ID
   * @param sequenceNumber a sequence number
   * @return a journal entry for the given container ID and sequence number
   */
  private static JournalEntry toEntry(long containerId, long sequenceNumber) {
    return JournalEntry.newBuilder().setInodeDirectoryIdGenerator(
        InodeDirectoryIdGeneratorEntry.newBuilder()
            .setContainerId(containerId)
            .setSequenceNumber(sequenceNumber))
        .build();
  }

  @Override
  public boolean processJournalEntry(JournalEntry entry) {
    if (entry.hasInodeDirectoryIdGenerator()) {
      InodeDirectoryIdGeneratorEntry e = entry.getInodeDirectoryIdGenerator();
      mNextDirectoryId.setContainerId(e.getContainerId());
      mNextDirectoryId.setSequenceNumber(e.getSequenceNumber());
      return true;
    }
    return false;
  }

  @Override
  public void resetState() {
    mNextDirectoryId.setContainerId(0);
    mNextDirectoryId.setSequenceNumber(0);
  }

  @Override
  public CloseableIterator<JournalEntry> getJournalEntryIterator() {
    return CloseableIterator.noopCloseable(CommonUtils.singleElementIterator(
        JournalEntry.newBuilder()
            .setInodeDirectoryIdGenerator(InodeDirectoryIdGeneratorEntry.newBuilder()
                .setContainerId(mNextDirectoryId.getContainerId())
                .setSequenceNumber(mNextDirectoryId.getSequenceNumber()))
        .build()));
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.INODE_DIRECTORY_ID_GENERATOR;
  }
}
