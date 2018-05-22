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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.eq;

import alluxio.AlluxioURI;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.LineageDeletionException;
import alluxio.exception.LineageDoesNotExistException;
import alluxio.job.CommandLineJob;
import alluxio.job.Job;
import alluxio.job.JobConf;
import alluxio.master.DefaultSafeModeManager;
import alluxio.master.MasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.SafeModeManager;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.noop.NoopJournalSystem;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Unit tests for {@link LineageMaster}.
 */
public final class LineageMasterTest {
  private ExecutorService mExecutorService;
  private LineageMaster mLineageMaster;
  private FileSystemMaster mFileSystemMaster;
  private Job mJob;
  private MasterRegistry mRegistry;
  private SafeModeManager mSafeModeManager;
  private long mStartTimeMs;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    mRegistry = new MasterRegistry();
    JournalSystem journalSystem = new NoopJournalSystem();
    mFileSystemMaster = mock(FileSystemMaster.class);
    mRegistry.add(FileSystemMaster.class, mFileSystemMaster);
    mSafeModeManager = new DefaultSafeModeManager();
    mStartTimeMs = System.currentTimeMillis();
    ThreadFactory threadPool = ThreadFactoryUtils.build("LineageMasterTest-%d", true);
    mExecutorService = Executors.newFixedThreadPool(2, threadPool);
    mLineageMaster = new DefaultLineageMaster(mFileSystemMaster,
        new MasterContext(journalSystem, mSafeModeManager, mStartTimeMs),
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
    mRegistry.add(LineageMaster.class, mLineageMaster);
    mJob = new CommandLineJob("test", new JobConf("output"));
  }

  @After
  public void after() throws Exception {
    mRegistry.stop();
  }

  /**
   * Tests the {@link LineageMaster#getLineageInfoList()} method.
   */
  @Test
  public void listLineages() throws Exception {
    when(mFileSystemMaster.getPath(anyLong())).thenReturn(new AlluxioURI("test"));
    mRegistry.start(true);
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
    when(mFileSystemMaster.getFileId(missingInput)).thenReturn(IdUtils.INVALID_FILE_ID);
    mRegistry.start(true);
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
    mRegistry.start(true);
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
    mRegistry.start(true);
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
   * {@link LineageMaster#deleteLineage(long, boolean)} without setting the {@code cascade}
   * flag to {@code true}.
   */
  @Test
  public void deleteLineageWithChildren() throws Exception {
    mRegistry.start(true);
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
    when(mFileSystemMaster.getFileInfo(any(Long.class))).thenReturn(fileInfo);
    mRegistry.start(true);
    mLineageMaster.createLineage(new ArrayList<AlluxioURI>(),
        Lists.newArrayList(new AlluxioURI("/test1")), mJob);
    mLineageMaster.reinitializeFile("/test1", 500L, 10L, TtlAction.DELETE);
    verify(mFileSystemMaster).reinitializeFile(new AlluxioURI("/test1"), 500L, 10L,
        TtlAction.DELETE);
  }

  /**
   * Tests that completing a file asynchronously works.
   */
  @Test
  public void asyncCompleteFile() throws Exception {
    mRegistry.start(true);
    AlluxioURI file = new AlluxioURI("/test1");
    mLineageMaster.createLineage(new ArrayList<AlluxioURI>(), Lists.newArrayList(file), mJob);
    mFileSystemMaster.completeFile(file, CompleteFileOptions.defaults());
    verify(mFileSystemMaster).completeFile(eq(file),
        any(CompleteFileOptions.class));
  }

  @Test
  public void stop() throws Exception {
    mRegistry.start(true);
    mRegistry.stop();
    Assert.assertTrue(mExecutorService.isShutdown());
    Assert.assertTrue(mExecutorService.isTerminated());
  }
}
