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
import alluxio.master.file.state.DirectoryId.UnmodifiableDirectoryId;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryIterable;
import alluxio.proto.journal.File.InodeDirectoryIdGeneratorEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Inode id management for directory inodes. Keep track of a block container id, along with a block
 * sequence number. If the block sequence number reaches the limit, a new block container id is
 * retrieved.
 */
@ThreadSafe
public class InodeDirectoryIdGenerator implements JournalEntryIterable {
  private final ContainerIdGenerable mContainerIdGenerator;

  private final InodeDirectoryIdGeneratorState mState;
  private final UnmodifiableDirectoryId mNextDirectoryId;
  private boolean mInitialized = false;

  /**
   * @param containerIdGenerator the container id generator to use
   */
  public InodeDirectoryIdGenerator(ContainerIdGenerable containerIdGenerator) {
    mContainerIdGenerator =
        Preconditions.checkNotNull(containerIdGenerator, "containerIdGenerator");
    mState = new InodeDirectoryIdGeneratorState();
    mNextDirectoryId = mState.getDirectoryId();
  }

  /**
   * @param entry a journal entry to apply
   */
  public void apply(JournalEntry entry) {
    mState.apply(entry);
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
    mState.applyAndJournal(context, toEntry(containerId, sequenceNumber));
    return directoryId;
  }

  private void initialize(JournalContext context) throws UnavailableException {
    if (!mInitialized) {
      mState.applyAndJournal(context, toEntry(mContainerIdGenerator.getNewContainerId(), 0));
      mInitialized = true;
    }
  }

  /**
   * @param containerId a container ID
   * @param sequenceNumber a sequence number
   * @return an inode directory journal entry for the given container ID and sequence number
   */
  private static InodeDirectoryIdGeneratorEntry toEntry(long containerId, long sequenceNumber) {
    return InodeDirectoryIdGeneratorEntry.newBuilder()
        .setContainerId(containerId)
        .setSequenceNumber(sequenceNumber)
        .build();
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    return CommonUtils.singleElementIterator(
        JournalEntry.newBuilder().setInodeDirectoryIdGenerator(
            InodeDirectoryIdGeneratorEntry.newBuilder()
                .setContainerId(mNextDirectoryId.getContainerId())
                .setSequenceNumber(mNextDirectoryId.getSequenceNumber())
                .build()
        ).build());
  }

  private static class InodeDirectoryIdGeneratorState {
    private final DirectoryId mNextDirectoryId = new DirectoryId();

    public UnmodifiableDirectoryId getDirectoryId() {
      return mNextDirectoryId.getUnmodifiableView();
    }

    public void applyAndJournal(Supplier<JournalContext> context,
        InodeDirectoryIdGeneratorEntry entry) {
      apply(entry);
      context.get().append(JournalEntry.newBuilder().setInodeDirectoryIdGenerator(entry).build());
    }

    public void apply(JournalEntry entry) {
      if (entry.hasInodeDirectoryIdGenerator()) {
        apply(entry.getInodeDirectoryIdGenerator());
      } else {
        throw new IllegalStateException("Unexpected journal entry: " + entry);
      }
    }

    private void apply(InodeDirectoryIdGeneratorEntry entry) {
      mNextDirectoryId.setContainerId(entry.getContainerId());
      mNextDirectoryId.setSequenceNumber(entry.getSequenceNumber());
    }

    public void reset() {
      mNextDirectoryId.setContainerId(0);
      mNextDirectoryId.setSequenceNumber(0);
    }
  }
}
