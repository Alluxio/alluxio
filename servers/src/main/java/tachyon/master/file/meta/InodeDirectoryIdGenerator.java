/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.file.meta;

import com.google.common.base.Preconditions;

import tachyon.master.block.BlockId;
import tachyon.master.block.ContainerIdGenerable;
import tachyon.master.journal.JournalEntryRepresentable;
import tachyon.proto.journal.File.InodeDirectoryIdGeneratorEntry;
import tachyon.proto.journal.Journal.JournalEntry;

/**
 * Inode id management for directory inodes. Keep track of a block container id, along with a block
 * sequence number. If the block sequence number reaches the limit, a new block container id is
 * retrieved.
 */
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
      mSequenceNumber ++;
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
  public void initFromJournalEntry(InodeDirectoryIdGeneratorEntry entry) {
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
