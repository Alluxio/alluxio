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

import java.io.IOException;

import com.google.common.base.Preconditions;

import tachyon.master.block.BlockId;
import tachyon.master.block.ContainerIdGenerator;
import tachyon.master.file.journal.InodeDirectoryIdGeneratorEntry;
import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalOutputStream;
import tachyon.master.journal.JournalSerializable;

/**
 * Inode id management for directory inodes. Keep track of a block container id, along with a
 * block sequence number. If the block sequence number reaches the limit, a new block container id
 * is retrieved.
 */
public class InodeDirectoryIdGenerator implements JournalSerializable {
  private final ContainerIdGenerator mContainerIdGenerator;

  private boolean mInitialized = false;
  private long mContainerId;
  private long mSequenceNumber;

  public InodeDirectoryIdGenerator(ContainerIdGenerator containerIdGenerator) {
    mContainerIdGenerator = Preconditions.checkNotNull(containerIdGenerator);
  }

  @Override
  public void writeToJournal(JournalOutputStream outputStream) throws IOException {
    outputStream.writeEntry(toJournalEntry());
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

  public synchronized JournalEntry toJournalEntry() {
    return new InodeDirectoryIdGeneratorEntry(mContainerId, mSequenceNumber);
  }

  public void fromJournalEntry(InodeDirectoryIdGeneratorEntry entry) {
    mContainerId = entry.getContainerId();
    mSequenceNumber = entry.getSequenceNumber();
  }

  private void initialize() {
    if (!mInitialized) {
      mContainerId = mContainerIdGenerator.getNewContainerId();
      mSequenceNumber = 0;
      mInitialized = true;
    }
  }
}
