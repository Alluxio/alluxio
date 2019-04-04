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

package alluxio.job.migrate;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.job.JobServerContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.underfs.UfsManager;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for
 * {@link MigrateDefinition#selectExecutors(MigrateConfig, List, SelectExecutorsContext)}.
 * No matter whether to delete source, selectExecutors should have the same behavior.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioBlockStore.class, FileSystemContext.class})
public final class MigrateDefinitionSelectExecutorsTest {
  private static final List<BlockWorkerInfo> BLOCK_WORKERS =
      new ImmutableList.Builder<BlockWorkerInfo>()
          .add(new BlockWorkerInfo(new WorkerNetAddress().setHost("host0"), 0, 0))
          .add(new BlockWorkerInfo(new WorkerNetAddress().setHost("host1"), 0, 0))
          .add(new BlockWorkerInfo(new WorkerNetAddress().setHost("host2"), 0, 0))
          .add(new BlockWorkerInfo(new WorkerNetAddress().setHost("host3"), 0, 0)).build();

  private static final WorkerInfo JOB_WORKER_0 =
      new WorkerInfo().setAddress(new WorkerNetAddress().setHost("host0"));
  private static final WorkerInfo JOB_WORKER_1 =
      new WorkerInfo().setAddress(new WorkerNetAddress().setHost("host1"));
  private static final WorkerInfo JOB_WORKER_2 =
      new WorkerInfo().setAddress(new WorkerNetAddress().setHost("host2"));
  private static final WorkerInfo JOB_WORKER_3 =
      new WorkerInfo().setAddress(new WorkerNetAddress().setHost("host3"));

  private static final List<WorkerInfo> JOB_WORKERS =
      ImmutableList.of(JOB_WORKER_0, JOB_WORKER_1, JOB_WORKER_2, JOB_WORKER_3);

  private FileSystem mMockFileSystem;
  private FileSystemContext mMockFileSystemContext;
  private AlluxioBlockStore mMockBlockStore;
  private UfsManager mMockUfsManager;

  @Before
  public void before() throws Exception {
    mMockFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    mMockBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
    mMockFileSystem = Mockito.mock(FileSystem.class);
    mMockUfsManager = Mockito.mock(UfsManager.class);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create(mMockFileSystemContext)).thenReturn(mMockBlockStore);
    when(mMockBlockStore.getAllWorkers()).thenReturn(BLOCK_WORKERS);
    createDirectory("/");
  }

  @Test
  public void migrateToSelf() throws Exception {
    createDirectory("/src");
    Assert.assertEquals(Maps.newHashMap(), assignMigrates("/src", "/src"));
  }

  @Test
  public void assignToLocalWorker() throws Exception {
    createFileWithBlocksOnWorkers("/src", 0);
    setPathToNotExist("/dst");
    Map<WorkerInfo, List<MigrateCommand>> expected = ImmutableMap.of(JOB_WORKERS.get(0),
        Collections.singletonList(new MigrateCommand("/src", "/dst")));
    Assert.assertEquals(expected, assignMigrates("/src", "/dst"));
  }

  @Test
  public void assignToWorkerWithMostBlocks() throws Exception {
    createFileWithBlocksOnWorkers("/src", 3, 1, 1, 3, 1);
    setPathToNotExist("/dst");
    Map<WorkerInfo, List<MigrateCommand>> expected = ImmutableMap.of(JOB_WORKERS.get(1),
        Collections.singletonList(new MigrateCommand("/src", "/dst")));
    Assert.assertEquals(expected, assignMigrates("/src", "/dst"));
  }

  @Test
  public void assignToLocalWorkerWithMostBlocksMultipleFiles() throws Exception {
    createDirectory("/dir");
    // Should go to worker 0.
    FileInfo info1 = createFileWithBlocksOnWorkers("/dir/src1", 0, 1, 2, 3, 0);
    // Should go to worker 2.
    FileInfo info2 = createFileWithBlocksOnWorkers("/dir/src2", 1, 1, 2, 2, 2);
    // Should go to worker 0.
    FileInfo info3 = createFileWithBlocksOnWorkers("/dir/src3", 2, 0, 0, 1, 1, 0);
    setChildren("/dir", info1, info2, info3);
    // Say the destination doesn't exist.
    setPathToNotExist("/dst");

    List<MigrateCommand> migrateCommandsWorker0 = Lists.newArrayList(
        new MigrateCommand("/dir/src1", "/dst/src1"), new MigrateCommand("/dir/src3", "/dst/src3"));
    List<MigrateCommand> migrateCommandsWorker2 =
        Lists.newArrayList(new MigrateCommand("/dir/src2", "/dst/src2"));
    ImmutableMap<WorkerInfo, List<MigrateCommand>> expected =
        ImmutableMap.of(JOB_WORKERS.get(0), migrateCommandsWorker0, JOB_WORKERS.get(2),
                migrateCommandsWorker2);
    Assert.assertEquals(expected, assignMigrates("/dir", "/dst"));
  }

  @Test
  public void migrateEmptyDirectory() throws Exception {
    createDirectory("/src");
    createDirectory("/dst");
    setPathToNotExist("/dst/src");
    assignMigrates("/src", "/dst/src");
    verify(mMockFileSystem).createDirectory(eq(new AlluxioURI("/dst/src")));
  }

  @Test
  public void migrateNestedEmptyDirectory() throws Exception {
    createDirectory("/src");
    FileInfo nested = createDirectory("/src/nested");
    setChildren("/src", nested);
    createDirectory("/dst");
    setPathToNotExist("/dst/src");
    assignMigrates("/src", "/dst/src");
    verify(mMockFileSystem).createDirectory(eq(new AlluxioURI("/dst/src/nested")));
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
  public void migrateMissingDestinationParent() throws Exception {
    createDirectory("/src");
    setPathToNotExist("/dst");
    setPathToNotExist("/dst/missing");
    try {
      assignMigratesFail("/src", "/dst/missing");
    } catch (Exception e) {
      Assert.assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/dst"), e.getMessage());
    }
  }

  @Test
  public void migrateIntoFile() throws Exception {
    createFile("/src");
    createFile("/dst");
    setPathToNotExist("/dst/src");
    try {
      assignMigratesFail("/src", "/dst/src");
    } catch (Exception e) {
      Assert.assertEquals(ExceptionMessage.MIGRATE_TO_FILE_AS_DIRECTORY.getMessage("/dst/src",
          "/dst"), e.getMessage());
    }
  }

  @Test
  public void migrateFileToDirectory() throws Exception {
    createFile("/src");
    createDirectory("/dst");
    try {
      assignMigratesFail("/src", "/dst");
    } catch (Exception e) {
      Assert.assertEquals(ExceptionMessage.MIGRATE_FILE_TO_DIRECTORY.getMessage("/src", "/dst"),
          e.getMessage());
    }
  }

  @Test
  public void migrateDirectoryToFile() throws Exception {
    createDirectory("/src");
    createFile("/dst");
    try {
      assignMigratesFail("/src", "/dst");
    } catch (Exception e) {
      Assert.assertEquals(ExceptionMessage.MIGRATE_DIRECTORY_TO_FILE.getMessage("/src", "/dst"),
          e.getMessage());
    }
  }

  @Test
  public void migrateFileToExistingDestinationWithoutOverwrite() throws Exception {
    createFile("/src");
    createFile("/dst");
    // Test with source being a file.
    try {
      assignMigratesFail("/src", "/dst");
    } catch (FileAlreadyExistsException e) {
      Assert.assertEquals(ExceptionMessage.MIGRATE_NEED_OVERWRITE.getMessage("/dst"),
          e.getMessage());
    }
  }

  @Test
  public void migrateDirectoryToExistingDestinationWithoutOverwrite() throws Exception {
    // Test with the source being a folder.
    createDirectory("/src");
    createDirectory("/dst");
    try {
      assignMigratesFail("/src", "/dst");
    } catch (FileAlreadyExistsException e) {
      Assert.assertEquals(ExceptionMessage.MIGRATE_NEED_OVERWRITE.getMessage("/dst"),
          e.getMessage());
    }
  }

  @Test
  public void migrateFileToExistingDestinationWithOverwrite() throws Exception {
    createFileWithBlocksOnWorkers("/src", 0);
    createFile("/dst");

    Map<WorkerInfo, List<MigrateCommand>> expected = ImmutableMap.of(JOB_WORKERS.get(0),
        Collections.singletonList(new MigrateCommand("/src", "/dst")));
    // Set overwrite to true.
    Assert.assertEquals(expected, assignMigrates(new MigrateConfig("/src", "/dst", "THROUGH",
        true, false)));
  }

  @Test
  public void migrateDirectoryIntoDirectoryWithOverwrite() throws Exception {
    createDirectory("/src");
    FileInfo nested = createDirectory("/src/nested");
    FileInfo moreNested = createDirectory("/src/nested/moreNested");
    FileInfo file1 = createFileWithBlocksOnWorkers("/src/file1", 2);
    FileInfo file2 = createFileWithBlocksOnWorkers("/src/nested/file2", 1);
    FileInfo file3 = createFileWithBlocksOnWorkers("/src/nested/moreNested/file3", 1);
    setChildren("/src", nested, file1);
    setChildren("/src/nested", moreNested, file2);
    setChildren("/src/nested/moreNested", file3);
    createDirectory("/dst");

    List<MigrateCommand> migrateCommandsWorker1 =
        Lists.newArrayList(new MigrateCommand("/src/nested/file2", "/dst/nested/file2"),
            new MigrateCommand("/src/nested/moreNested/file3",
                    "/dst/nested/moreNested/file3"));
    List<MigrateCommand> migrateCommandsWorker2 =
        Lists.newArrayList(new MigrateCommand("/src/file1", "/dst/file1"));
    ImmutableMap<WorkerInfo, List<MigrateCommand>> expected =
        ImmutableMap.of(JOB_WORKERS.get(1), migrateCommandsWorker1, JOB_WORKERS.get(2),
                migrateCommandsWorker2);
    Assert.assertEquals(expected, assignMigrates(new MigrateConfig(
            "/src", "/dst", "THROUGH", true, false)));
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

    Map<WorkerInfo, ArrayList<MigrateCommand>> assignments =
        new MigrateDefinition().selectExecutors(
            new MigrateConfig("/src", "/dst", "THROUGH", true, false),
            ImmutableList.of(JOB_WORKER_3),
            new SelectExecutorsContext(1,
                new JobServerContext(mMockFileSystem, mMockFileSystemContext, mMockUfsManager)));

    Assert.assertEquals(ImmutableMap.of(JOB_WORKER_3,
        new ArrayList<>(Arrays.asList(new MigrateCommand("/src", "/dst")))), assignments);
  }

  /**
   * Runs selectExecutors for the migrate from source to destination.
   */
  private Map<WorkerInfo, ArrayList<MigrateCommand>> assignMigrates(String source,
      String destination) throws Exception {
    return assignMigrates(new MigrateConfig(source, destination, "THROUGH", false, false));
  }

  /**
   * Runs selectExecutors for the migrate from source to destination with the given writeType and
   * overwrite value.
   */
  private Map<WorkerInfo, ArrayList<MigrateCommand>> assignMigrates(MigrateConfig config)
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
    Map<WorkerInfo, ArrayList<MigrateCommand>> assignment =
        assignMigrates(new MigrateConfig(source, destination, writeType, overwrite, false));
    Assert.fail(
        "Selecting executors should have failed, but it succeeded with assignment " + assignment);
  }

  private void createFile(String name) throws Exception {
    createFileWithBlocksOnWorkers(name);
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
   * Informs the mock that the given fileInfos are children of the parent.
   */
  private void setChildren(String parent, FileInfo... children) throws Exception {
    List<URIStatus> statuses = new ArrayList<>();
    for (FileInfo child : children) {
      statuses.add(new URIStatus(child));
    }
    when(mMockFileSystem.listStatus(new AlluxioURI(parent)))
        .thenReturn(Lists.newArrayList(statuses));
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
