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

package alluxio.master.lineage;

import alluxio.AlluxioURI;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.LineageDeletionException;
import alluxio.exception.LineageDoesNotExistException;
import alluxio.job.CommandLineJob;
import alluxio.job.Job;
import alluxio.job.JobConf;
import alluxio.master.MasterRegistry;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.journal.JournalFactory;
import alluxio.master.journal.MutableJournal;
import alluxio.util.IdUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.FileInfo;
import alluxio.wire.LineageInfo;
import alluxio.wire.TtlAction;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Unit tests for {@link LineageMaster}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class})
public final class LineageMasterTest {
  private ExecutorService mExecutorService;
  private LineageMaster mLineageMaster;
  private FileSystemMaster mFileSystemMaster;
  private Job mJob;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    MasterRegistry registry = new MasterRegistry();
    JournalFactory factory =
        new MutableJournal.Factory(new URI(mTestFolder.newFolder().getAbsolutePath()));
    mFileSystemMaster = Mockito.mock(FileSystemMaster.class);
    registry.add(FileSystemMaster.class, mFileSystemMaster);
    ThreadFactory threadPool = ThreadFactoryUtils.build("LineageMasterTest-%d", true);
    mExecutorService = Executors.newFixedThreadPool(2, threadPool);
    mLineageMaster = new LineageMaster(registry, factory,
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
    mJob = new CommandLineJob("test", new JobConf("output"));
  }

  @After
  public void after() throws Exception {
    mLineageMaster.stop();
  }

  /**
   * Tests the {@link LineageMaster#getLineageInfoList()} method.
   */
  @Test
  public void listLineages() throws Exception {
    Mockito.when(mFileSystemMaster.getPath(Mockito.anyLong())).thenReturn(new AlluxioURI("test"));
    mLineageMaster.start(true);
    mLineageMaster.createLineage(new ArrayList<AlluxioURI>(),
        Lists.newArrayList(new AlluxioURI("/test1")), mJob);
    mLineageMaster.createLineage(Lists.newArrayList(new AlluxioURI("/test1")),
        Lists.newArrayList(new AlluxioURI("/test2")), mJob);
    List<LineageInfo> info = mLineageMaster.getLineageInfoList();
    Assert.assertEquals(2, info.size());
  }

  /**
   * Tests that an exception is thrown when trying to create a lineage for a non-existing file via
   * the {@link LineageMaster#createLineage(List, List, Job)} method.
   */
  @Test
  public void createLineageWithNonExistingFile() throws Exception {
    AlluxioURI missingInput = new AlluxioURI("/test1");
    Mockito.when(mFileSystemMaster.getFileId(missingInput)).thenReturn(IdUtils.INVALID_FILE_ID);
    mLineageMaster.start(true);
    // try catch block used because ExpectedExceptionRule conflicts with Powermock
    try {
      mLineageMaster.createLineage(Lists.newArrayList(missingInput),
          Lists.newArrayList(new AlluxioURI("/test2")), mJob);
      Assert.fail();
    } catch (FileDoesNotExistException e) {
      Assert.assertEquals(ExceptionMessage.LINEAGE_INPUT_FILE_NOT_EXIST.getMessage("/test1"),
          e.getMessage());
    }
  }

  /**
   * Tests the {@link LineageMaster#deleteLineage(long, boolean)} method.
   */
  @Test
  public void deleteLineage() throws Exception {
    mLineageMaster.start(true);
    long l1 = mLineageMaster.createLineage(new ArrayList<AlluxioURI>(),
        Lists.newArrayList(new AlluxioURI("/test1")), mJob);
    mLineageMaster.createLineage(Lists.newArrayList(new AlluxioURI("/test1")),
        Lists.newArrayList(new AlluxioURI("/test2")), mJob);
    mLineageMaster.deleteLineage(l1, true);
    List<LineageInfo> info = mLineageMaster.getLineageInfoList();
    Assert.assertEquals(0, info.size());
  }

  /**
   * Tests that an exception is thrown when trying to delete a non-existing lineage via the
   * {@link LineageMaster#deleteLineage(long, boolean)} method.
   */
  @Test
  public void deleteNonexistingLineage() throws Exception {
    mLineageMaster.start(true);
    long id = 1L;
    try {
      mLineageMaster.deleteLineage(id, false);
      Assert.fail();
    } catch (LineageDoesNotExistException e) {
      Assert.assertEquals(ExceptionMessage.LINEAGE_DOES_NOT_EXIST.getMessage(id), e.getMessage());
    }
  }

  /**
   * Tests that an exception is thrown when trying to delete a lineage with children via the
   * {@link LineageMaster#deleteLineage(long, boolean)} without setting the {@code cascade} flag to
   * {@code true}.
   */
  @Test
  public void deleteLineageWithChildren() throws Exception {
    mLineageMaster.start(true);
    long l1 = mLineageMaster.createLineage(new ArrayList<AlluxioURI>(),
        Lists.newArrayList(new AlluxioURI("/test1")), mJob);
    mLineageMaster.createLineage(Lists.newArrayList(new AlluxioURI("/test1")),
        Lists.newArrayList(new AlluxioURI("/test2")), mJob);
    try {
      mLineageMaster.deleteLineage(l1, false);
      Assert.fail();
    } catch (LineageDeletionException e) {
      Assert.assertEquals(ExceptionMessage.DELETE_LINEAGE_WITH_CHILDREN.getMessage(l1),
          e.getMessage());
    }
  }

  /**
   * Tests the {@link LineageMaster#reinitializeFile(String, long, long, TtlAction)} method.
   */
  @Test
  public void reinitializeFile() throws Exception {
    FileInfo fileInfo = new FileInfo();
    fileInfo.setCompleted(false);
    Mockito.when(mFileSystemMaster.getFileInfo(Mockito.any(Long.class))).thenReturn(fileInfo);
    mLineageMaster.start(true);
    mLineageMaster.createLineage(new ArrayList<AlluxioURI>(),
        Lists.newArrayList(new AlluxioURI("/test1")), mJob);
    mLineageMaster.reinitializeFile("/test1", 500L, 10L, TtlAction.DELETE);
    Mockito.verify(mFileSystemMaster).reinitializeFile(new AlluxioURI("/test1"), 500L, 10L,
        TtlAction.DELETE);
  }

  /**
   * Tests that completing a file asynchronously works.
   */
  @Test
  public void asyncCompleteFile() throws Exception {
    mLineageMaster.start(true);
    AlluxioURI file = new AlluxioURI("/test1");
    mLineageMaster.createLineage(new ArrayList<AlluxioURI>(), Lists.newArrayList(file), mJob);
    mFileSystemMaster.completeFile(file, CompleteFileOptions.defaults());
    Mockito.verify(mFileSystemMaster).completeFile(Mockito.eq(file),
        Mockito.any(CompleteFileOptions.class));
  }

  @Test
  public void stop() throws Exception {
    mLineageMaster.start(true);
    mLineageMaster.stop();
    Assert.assertTrue(mExecutorService.isShutdown());
    Assert.assertTrue(mExecutorService.isTerminated());
  }
}
