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

package alluxio.master.lineage.meta;

import alluxio.master.journal.JournalEntryRepresentable;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.journal.Lineage.LineageIdGeneratorEntry;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

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
  long generateId() {
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
