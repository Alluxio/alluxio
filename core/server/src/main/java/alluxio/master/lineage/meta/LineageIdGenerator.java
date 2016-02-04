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

package alluxio.master.lineage.meta;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

import alluxio.master.journal.JournalEntryRepresentable;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.journal.Lineage.LineageIdGeneratorEntry;

/**
 * Generates the lineage id as sequence number.
 */
@ThreadSafe
public final class LineageIdGenerator implements JournalEntryRepresentable {
  private AtomicLong mSequenceNumber;

  /**
   * Creates a new instance of {@link LineageIdGenerator}.
   */
  public LineageIdGenerator() {
    mSequenceNumber = new AtomicLong(0);
  }

  /**
   * Generates a new id for lineage.
   *
   * @return the new id for lineage
   */
  synchronized long generateId() {
    return mSequenceNumber.getAndIncrement();
  }

  /**
   * Constructs the generator from a journal entry.
   *
   * @param entry the journal entry
   */
  public void initFromJournalEntry(LineageIdGeneratorEntry entry) {
    mSequenceNumber = new AtomicLong(entry.getSequenceNumber());
  }

  @Override
  public JournalEntry toJournalEntry() {
    LineageIdGeneratorEntry lineageIdGenerator = LineageIdGeneratorEntry.newBuilder()
        .setSequenceNumber(mSequenceNumber.longValue())
        .build();
    return JournalEntry.newBuilder().setLineageIdGenerator(lineageIdGenerator).build();
  }
}
