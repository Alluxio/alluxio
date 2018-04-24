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

import alluxio.proto.journal.Journal.JournalEntry;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link LineageIdGenerator}.
 */
public final class LineageIdGeneratorTest {

  /**
   * Tests the {@link LineageIdGenerator#initFromJournalEntry(
   * alluxio.proto.journal.Lineage.LineageIdGeneratorEntry)} method.
   */
  @Test
  public void journalEntrySerialization() {
    LineageIdGenerator generator = new LineageIdGenerator();
    long id = generator.generateId();
    JournalEntry entry = generator.toJournalEntry();
    generator = new LineageIdGenerator();
    generator.initFromJournalEntry(entry.getLineageIdGenerator());
    Assert.assertEquals(id + 1, generator.generateId());
  }
}
