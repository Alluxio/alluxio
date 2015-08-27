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

package tachyon.master.next.block;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import tachyon.master.next.block.journal.BlockIdGeneratorEntry;
import tachyon.master.next.journal.JournalOutputStream;
import tachyon.master.next.journal.JournalSerializable;

public class BlockIdGenerator implements JournalSerializable {

  private final AtomicLong mNextContainerId;

  // TODO: when needed, add functionality to create new full block ids.

  public BlockIdGenerator() {
    mNextContainerId = new AtomicLong(0);
  }

  public synchronized long getNewBlockContainerId() {
    return mNextContainerId.getAndIncrement();
  }

  public synchronized void setNextContainerId(long id) {
    mNextContainerId.set(id);
  }

  @Override
  public synchronized void writeToJournal(JournalOutputStream outputStream) throws IOException {
    outputStream.writeEntry(new BlockIdGeneratorEntry(mNextContainerId.get()));
  }
}
