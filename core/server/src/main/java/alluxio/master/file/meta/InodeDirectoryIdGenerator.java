/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.meta;

import alluxio.master.block.BlockId;
import alluxio.master.block.ContainerIdGenerable;
import alluxio.master.journal.JournalEntryRepresentable;
import alluxio.proto.journal.File.InodeDirectoryIdGeneratorEntry;
import alluxio.proto.journal.Journal.JournalEntry;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Inode id management for directory inodes. Keep track of a block container id, along with a block
 * sequence number. If the block sequence number reaches the limit, a new block container id is
 * retrieved.
 */
@ThreadSafe
public class InodeDirectoryIdGenerator implements JournalEntryRepresentable {
  private final ContainerIdGenerable mContainerIdGenerator;

  private boolean mInitialized = false;
  private long mContainerId;
  private long mSequenceNumber;

  /**
   * @param containerIdGenerator the container id generator to use
   */
  public InodeDirectoryIdGenerator(ContainerIdGenerable containerIdGenerator) {
    mContainerIdGenerator = Preconditions.checkNotNull(containerIdGenerator);
  }

  synchronized long getNewDirectoryId() {
    initialize();
    long directoryId = BlockId.createBlockId(mContainerId, mSequenceNumber);
    if (mSequenceNumber == BlockId.getMaxSequenceNumber()) {
      // No more ids in this container. Get a new container for the next id.
      mContainerId = mContainerIdGenerator.getNewContainerId();
      mSequenceNumber = 0;
    } else {
      mSequenceNumber++;
    }
    return directoryId;
  }

  @Override
  public synchronized JournalEntry toJournalEntry() {
    InodeDirectoryIdGeneratorEntry inodeDirectoryIdGenerator =
        InodeDirectoryIdGeneratorEntry.newBuilder()
        .setContainerId(mContainerId)
        .setSequenceNumber(mSequenceNumber)
        .build();
    return JournalEntry.newBuilder()
        .setInodeDirectoryIdGenerator(inodeDirectoryIdGenerator)
        .build();
  }

  /**
   * Initializes the object using a journal entry.
   *
   * @param entry {@link InodeDirectoryIdGeneratorEntry} to use for initialization
   */
  public synchronized void initFromJournalEntry(InodeDirectoryIdGeneratorEntry entry) {
    mContainerId = entry.getContainerId();
    mSequenceNumber = entry.getSequenceNumber();
    mInitialized = true;
  }

  private void initialize() {
    if (!mInitialized) {
      mContainerId = mContainerIdGenerator.getNewContainerId();
      mSequenceNumber = 0;
      mInitialized = true;
    }
  }
}
