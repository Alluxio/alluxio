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

package tachyon.master.block;

import java.util.concurrent.atomic.AtomicLong;

import tachyon.master.journal.JournalEntryRepresentable;
import tachyon.proto.journal.Block.BlockContainerIdGeneratorEntry;
import tachyon.proto.journal.Journal.JournalEntry;

/**
 * This class generates unique block container ids.
 */
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
