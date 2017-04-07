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

import alluxio.exception.ExceptionMessage;
import alluxio.exception.LineageDoesNotExistException;
import alluxio.job.CommandLineJob;
import alluxio.job.Job;
import alluxio.job.JobConf;
import alluxio.proto.journal.Journal;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Unit tests for {@link LineageStore}.
 */
public final class LineageStoreTest {
  private LineageStore mLineageStore;
  private Job mJob;

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public void before() {
    mLineageStore = new LineageStore(new LineageIdGenerator());
    mJob = new CommandLineJob("test", new JobConf("output"));
  }

  /**
   * Tests the {@link LineageStore#createLineage(List, List, Job)} method.
   */
  @Test
  public void createLineage() {
    long l1 = mLineageStore.createLineage(new ArrayList<Long>(), Lists.newArrayList(1L), mJob);
    long l2 = mLineageStore.createLineage(Lists.newArrayList(1L), Lists.newArrayList(2L), mJob);
    long l3 = mLineageStore.createLineage(Lists.newArrayList(2L), Lists.newArrayList(3L), mJob);
    List<Lineage> lineages = mLineageStore.getAllInTopologicalOrder();
    Assert.assertEquals(l1, lineages.get(0).getId());
    Assert.assertEquals(l2, lineages.get(1).getId());
    Assert.assertEquals(l3, lineages.get(2).getId());
    Assert.assertEquals(1, mLineageStore.getRootLineages().size());
  }

  /**
   * Tests the {@link LineageStore#deleteLineage(long)} method.
   */
  @Test
  public void deleteLineage() throws Exception {
    long l1 = mLineageStore.createLineage(new ArrayList<Long>(), Lists.newArrayList(1L), mJob);
    long l2 = mLineageStore.createLineage(Lists.newArrayList(1L), Lists.newArrayList(2L), mJob);
    // delete the root
    mLineageStore.deleteLineage(l1);
    // neither exists
    Assert.assertNull(mLineageStore.getLineage(l1));
    Assert.assertNull(mLineageStore.getLineage(l2));
  }

  /**
   * Tests that an exception is thrown when trying to delete a non-existing lineage via the
   * {@link LineageStore#deleteLineage(long)} method.
   */
  @Test
  public void deleteNonexistingLineage() throws Exception {
    long id = 1;
    mThrown.expect(LineageDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.LINEAGE_DOES_NOT_EXIST.getMessage(id));

    mLineageStore.deleteLineage(id);
  }

  /**
   * Tests the {@link LineageStore#getJournalEntryIterator()}} method.
   */
  @Test
  public void journalEntrySerialization() throws Exception {
    long l1 = mLineageStore.createLineage(new ArrayList<Long>(), Lists.newArrayList(1L), mJob);
    long l2 = mLineageStore.createLineage(Lists.newArrayList(1L), Lists.newArrayList(2L),
        mJob);

    Iterator<Journal.JournalEntry> it = mLineageStore.getJournalEntryIterator();
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(mLineageStore.getLineage(l1).toJournalEntry(), it.next());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(mLineageStore.getLineage(l2).toJournalEntry(), it.next());
  }
}
