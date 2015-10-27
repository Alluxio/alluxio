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

package tachyon.master.lineage.meta;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import tachyon.client.file.TachyonFile;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.LineageDoesNotExistException;
import tachyon.exception.PreconditionMessage;
import tachyon.job.CommandLineJob;
import tachyon.job.Job;
import tachyon.job.JobConf;
import tachyon.master.journal.JournalOutputStream;

public final class LineageStoreTest {
  private LineageStore mLineageStore;
  private Job mJob;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() {
    mLineageStore = new LineageStore(new LineageIdGenerator());
    mJob = new CommandLineJob("test", new JobConf("output"));
  }

  @Test
  public void createLineageTest() {
    long l1 = mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(),
        Lists.newArrayList(new LineageFile(1)), mJob);
    long l2 = mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(new TachyonFile(1)),
        Lists.newArrayList(new LineageFile(2)), mJob);
    long l3 = mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(new TachyonFile(2)),
        Lists.newArrayList(new LineageFile(3)), mJob);
    List<Lineage> lineages = mLineageStore.getAllInTopologicalOrder();
    Assert.assertEquals(l1, lineages.get(0).getId());
    Assert.assertEquals(l2, lineages.get(1).getId());
    Assert.assertEquals(l3, lineages.get(2).getId());
    Assert.assertEquals(1, mLineageStore.getRootLineages().size());
  }

  @Test
  public void completeFileForAsyncWriteTest() {
    long id = mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(),
        Lists.newArrayList(new LineageFile(1)), mJob);
    mLineageStore.completeFile(1);
    Assert.assertEquals(LineageFileState.COMPLETED,
        mLineageStore.getLineage(id).getOutputFiles().get(0).getState());
  }

  @Test
  public void deleteLineageTest() {
    long l1 = mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(),
        Lists.newArrayList(new LineageFile(1)), mJob);
    long l2 = mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(new TachyonFile(1)),
        Lists.newArrayList(new LineageFile(2)), mJob);
    // delete the root
    mLineageStore.deleteLineage(l1);
    // neither exists
    Assert.assertNull(mLineageStore.getLineage(l1));
    Assert.assertNull(mLineageStore.getLineage(l2));
  }

  @Test
  public void deleteNonexistingLineageTest() {
    long id = 1;
    mThrown.expect(IllegalStateException.class);
    mThrown.expectMessage(String.format(PreconditionMessage.LINEAGE_NOT_EXIST, id));

    mLineageStore.deleteLineage(id);
  }

  @Test
  public void requestNonexistingFileForPersistenceTest() {
    long fileId = 1;
    mThrown.expect(IllegalStateException.class);
    mThrown.expectMessage(String.format(PreconditionMessage.LINEAGE_NO_OUTPUT_FILE, fileId));

    mLineageStore.requestFilePersistence(fileId);
  }

  @Test
  public void reportLostFileTest() throws LineageDoesNotExistException {
    long fileId = 1;
    mThrown.expect(LineageDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.LINEAGE_OUTPUT_FILE_NOT_EXIST.getMessage(fileId));

    mLineageStore.reportLostFile(fileId);
  }

  @Test
  public void commitNonexistingFilePersistenceTest() {
    long fileId = 1;
    mThrown.expect(IllegalStateException.class);
    mThrown.expectMessage(String.format(PreconditionMessage.LINEAGE_NO_OUTPUT_FILE, fileId));

    mLineageStore.commitFilePersistence(fileId);
  }

  @Test
  public void journalEntrySerializationTest() throws IOException {
    long l1 = mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(),
        Lists.newArrayList(new LineageFile(1)), mJob);
    long l2 = mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(new TachyonFile(1)),
        Lists.newArrayList(new LineageFile(2)), mJob);

    JournalOutputStream outputStream = Mockito.mock(JournalOutputStream.class);
    mLineageStore.streamToJournalCheckpoint(outputStream);
    Mockito.verify(outputStream).writeEntry(mLineageStore.getLineage(l1).toJournalEntry());
    Mockito.verify(outputStream).writeEntry(mLineageStore.getLineage(l2).toJournalEntry());
  }
}
