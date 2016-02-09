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

import alluxio.exception.ExceptionMessage;
import alluxio.exception.LineageDoesNotExistException;
import alluxio.job.CommandLineJob;
import alluxio.job.Job;
import alluxio.job.JobConf;
import alluxio.master.journal.JournalOutputStream;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

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
  public void createLineageTest() {
    long l1 = mLineageStore.createLineage(Lists.<Long>newArrayList(), Lists.newArrayList(1L), mJob);
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
   *
   * @throws Exception if deleting the lineage fails
   */
  @Test
  public void deleteLineageTest() throws Exception {
    long l1 = mLineageStore.createLineage(Lists.<Long>newArrayList(), Lists.newArrayList(1L), mJob);
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
   *
   * @throws Exception if deleting the lineage fails
   */
  @Test
  public void deleteNonexistingLineageTest() throws Exception {
    long id = 1;
    mThrown.expect(LineageDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.LINEAGE_DOES_NOT_EXIST.getMessage(id));

    mLineageStore.deleteLineage(id);
  }

  /**
   * Tests the {@link LineageStore#streamToJournalCheckpoint(JournalOutputStream)} method.
   *
   * @throws Exception if a {@link LineageStore} operation fails
   */
  @Test
  public void journalEntrySerializationTest() throws Exception {
    long l1 = mLineageStore.createLineage(Lists.<Long>newArrayList(), Lists.newArrayList(1L), mJob);
    long l2 =
        mLineageStore.createLineage(Lists.<Long>newArrayList(1L), Lists.newArrayList(2L), mJob);

    JournalOutputStream outputStream = Mockito.mock(JournalOutputStream.class);
    mLineageStore.streamToJournalCheckpoint(outputStream);
    Mockito.verify(outputStream).writeEntry(mLineageStore.getLineage(l1).toJournalEntry());
    Mockito.verify(outputStream).writeEntry(mLineageStore.getLineage(l2).toJournalEntry());
  }
}
