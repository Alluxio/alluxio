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

package alluxio.job.plan.migrate;

import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.job.JobServerContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.plan.SelectExecutorsTest;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.Set;

/**
 * Unit tests for
 * {@link MigrateDefinition#selectExecutors(MigrateConfig, List, SelectExecutorsContext)}.
 * No matter whether to delete source, selectExecutors should have the same behavior.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioBlockStore.class})
public final class MigrateDefinitionSelectExecutorsTest extends SelectExecutorsTest {
  private static final List<BlockWorkerInfo> BLOCK_WORKERS =
      new ImmutableList.Builder<BlockWorkerInfo>()
          .add(new BlockWorkerInfo(new WorkerNetAddress().setHost("host0"), 0, 0))
          .add(new BlockWorkerInfo(new WorkerNetAddress().setHost("host1"), 0, 0))
          .add(new BlockWorkerInfo(new WorkerNetAddress().setHost("host2"), 0, 0))
          .add(new BlockWorkerInfo(new WorkerNetAddress().setHost("host3"), 0, 0)).build();

  private AlluxioBlockStore mMockBlockStore;

  @Before
  @Override
  public void before() throws Exception {
    super.before();
    mMockBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create(mMockFileSystemContext)).thenReturn(mMockBlockStore);
    when(mMockFileSystemContext.getCachedWorkers()).thenReturn(BLOCK_WORKERS);
    createDirectory("/");
  }

  @Test
  public void migrateToSelf() throws Exception {
    createDirectory("/src");
    Assert.assertEquals(ImmutableSet.of(), assignMigrates("/src", "/src"));
  }

  @Test
  public void assignToLocalWorker() throws Exception {
    createFileWithBlocksOnWorkers("/src", 0);
    setPathToNotExist("/dst");
    Set<Pair<WorkerInfo, MigrateCommand>> expected = ImmutableSet.of(new Pair<>(
        JOB_WORKERS.get(0), new MigrateCommand("/src", "/dst")));
    Assert.assertEquals(expected, assignMigrates("/src", "/dst"));
  }

  @Test
  public void assignToWorkerWithMostBlocks() throws Exception {
    createFileWithBlocksOnWorkers("/src", 3, 1, 1, 3, 1);
    setPathToNotExist("/dst");
    Set<Pair<WorkerInfo, MigrateCommand>> expected = ImmutableSet.of(new Pair<>(
        JOB_WORKERS.get(1), new MigrateCommand("/src", "/dst")));
    Assert.assertEquals(expected, assignMigrates("/src", "/dst"));
  }

  @Test
  public void migrateToSubpath() throws Exception {
    try {
      assignMigratesFail("/src", "/src/dst");
    } catch (RuntimeException e) {
      Assert.assertEquals(
          ExceptionMessage.MIGRATE_CANNOT_BE_TO_SUBDIRECTORY.getMessage("/src", "/src/dst"),
          e.getMessage());
    }
  }

  @Test
  public void migrateMissingSource() throws Exception {
    setPathToNotExist("/notExist");
    try {
      assignMigratesFail("/notExist", "/dst");
    } catch (FileDoesNotExistException e) {
      Assert.assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/notExist"),
          e.getMessage());
    }
  }

  @Test
  public void migrateUncachedFile() throws Exception {
    createFileWithBlocksOnWorkers("/src");
    setPathToNotExist("/dst");
    Assert.assertEquals(1, assignMigrates("/src", "/dst").size());
  }

  @Test
  public void migrateNoLocalJobWorker() throws Exception {
    createFileWithBlocksOnWorkers("/src", 0);
    setPathToNotExist("/dst");

    Set<Pair<WorkerInfo, MigrateCommand>> assignments =
        new MigrateDefinition().selectExecutors(
            new MigrateConfig("/src", "/dst", "THROUGH", true),
            ImmutableList.of(JOB_WORKER_3),
            new SelectExecutorsContext(1,
                new JobServerContext(mMockFileSystem, mMockFileSystemContext, mMockUfsManager)));

    Assert.assertEquals(ImmutableSet.of(new Pair<>(JOB_WORKER_3,
        new MigrateCommand("/src", "/dst"))), assignments);
  }

  /**
   * Runs selectExecutors for the migrate from source to destination.
   */
  private Set<Pair<WorkerInfo, MigrateCommand>> assignMigrates(String source,
      String destination) throws Exception {
    return assignMigrates(new MigrateConfig(source, destination, "THROUGH", false));
  }

  /**
   * Runs selectExecutors for the migrate from source to destination with the given writeType and
   * overwrite value.
   */
  private Set<Pair<WorkerInfo, MigrateCommand>> assignMigrates(MigrateConfig config)
      throws Exception {
    return new MigrateDefinition().selectExecutors(config,
        JOB_WORKERS, new SelectExecutorsContext(1,
            new JobServerContext(mMockFileSystem, mMockFileSystemContext, mMockUfsManager)));
  }

  /**
   * Runs selectExecutors with the expectation that it will throw an exception.
   */
  private void assignMigratesFail(String source, String destination) throws Exception {
    assignMigratesFail(source, destination, "THROUGH", false);
  }

  /**
   * Runs selectExecutors with the expectation that it will throw an exception.
   */
  private void assignMigratesFail(String source, String destination, String writeType,
      boolean overwrite) throws Exception {
    Set<Pair<WorkerInfo, MigrateCommand>> assignment =
        assignMigrates(new MigrateConfig(source, destination, writeType, overwrite));
    Assert.fail(
        "Selecting executors should have failed, but it succeeded with assignment " + assignment);
  }

  private FileInfo createFileWithBlocksOnWorkers(String testFile, int... workerInds)
      throws Exception {
    return createFileWithBlocksOnWorkers(testFile, new FileInfo(), workerInds);
  }

  /**
   * Creates a file with the given name and a block on each specified worker. Workers may be
   * repeated to give them multiple blocks.
   *
   * @param testFile the name of the file to create
   * @param fileInfo file info to apply to the created file
   * @param workerInds the workers to put blocks on, specified by their indices
   * @return file info for the created file
   */
  private FileInfo createFileWithBlocksOnWorkers(String testFile, FileInfo fileInfo,
      int... workerInds) throws Exception {
    AlluxioURI uri = new AlluxioURI(testFile);
    List<FileBlockInfo> blockInfos = Lists.newArrayList();
    for (int workerInd : workerInds) {
      WorkerNetAddress address = JOB_WORKERS.get(workerInd).getAddress();
      blockInfos.add(new FileBlockInfo().setBlockInfo(new BlockInfo()
          .setLocations(Lists.newArrayList(new BlockLocation().setWorkerAddress(address)))));
    }
    FileInfo testFileInfo =
        fileInfo.setFolder(false).setPath(testFile).setFileBlockInfos(blockInfos);
    when(mMockFileSystem.listStatus(uri))
        .thenReturn(Lists.newArrayList(new URIStatus(testFileInfo)));
    when(mMockFileSystem.getStatus(uri)).thenReturn(new URIStatus(testFileInfo));
    return testFileInfo;
  }

  /**
   * Creates a directory with the given name.
   *
   * @return file info for the created directory
   */
  private FileInfo createDirectory(String name) throws Exception {
    // Call all directories mount points to force cross-mount functionality.
    FileInfo info = new FileInfo().setFolder(true).setPath(name).setMountPoint(true);
    when(mMockFileSystem.getStatus(new AlluxioURI(name))).thenReturn(new URIStatus(info));
    return info;
  }

  /**
   * Tells the mock to throw FileDoesNotExistException when the given path is queried.
   */
  private void setPathToNotExist(String path) throws Exception {
    AlluxioURI uri = new AlluxioURI(path);
    when(mMockFileSystem.getStatus(uri)).thenThrow(new FileDoesNotExistException(uri));
    when(mMockFileSystem.listStatus(uri)).thenThrow(new FileDoesNotExistException(uri));
  }
}
