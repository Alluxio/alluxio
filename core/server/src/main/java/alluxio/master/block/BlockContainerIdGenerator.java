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

package alluxio.master.block;

import alluxio.master.journal.JournalEntryRepresentable;
import alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry;
import alluxio.proto.journal.Journal.JournalEntry;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class generates unique block container ids.
 */
@ThreadSafe
public final class BlockContainerIdGenerator
    implements JournalEntryRepresentable, ContainerIdGenerable {

  private final AtomicLong mNextContainerId;

  /**
   * Creates a new instance of {@link BlockContainerIdGenerator}.
   */
  public BlockContainerIdGenerator() {
    mNextContainerId = new AtomicLong(0);
  }

  @Override
  public synchronized long getNewContainerId() {
    return mNextContainerId.getAndIncrement();
  }

  /**
   * @param id the next container id to use
   */
  public synchronized void setNextContainerId(long id) {
    mNextContainerId.set(id);
  }

  @Override
  public synchronized JournalEntry toJournalEntry() {
    BlockContainerIdGeneratorEntry blockContainerIdGenerator =
        BlockContainerIdGeneratorEntry.newBuilder()
        .setNextContainerId(mNextContainerId.get())
        .build();
    return JournalEntry.newBuilder()
        .setBlockContainerIdGenerator(blockContainerIdGenerator)
        .build();
  }
}
