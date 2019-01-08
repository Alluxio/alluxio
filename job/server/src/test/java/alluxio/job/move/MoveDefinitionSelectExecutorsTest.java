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

package alluxio.job.move;

import static org.mockito.Matchers.any;
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
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.job.JobMasterContext;
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
import org.mockito.Matchers;
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
 * Unit tests for {@link MoveDefinition#selectExecutors(MoveConfig, List, JobMasterContext)}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioBlockStore.class, FileSystemContext.class})
public final class MoveDefinitionSelectExecutorsTest {
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
  public void moveToSelf() throws Exception {
    createDirectory("/src");
    Assert.assertEquals(Maps.newHashMap(), assignMoves("/src", "/src"));
  }

  @Test
  public void assignToLocalWorker() throws Exception {
    createFileWithBlocksOnWorkers("/src", 0);
    setPathToNotExist("/dst");
    Map<WorkerInfo, List<MoveCommand>> expected = ImmutableMap.of(JOB_WORKERS.get(0),
        Collections.singletonList(new MoveCommand("/src", "/dst")));
    Assert.assertEquals(expected, assignMoves("/src", "/dst"));
  }

  @Test
  public void assignToWorkerWithMostBlocks() throws Exception {
    createFileWithBlocksOnWorkers("/src", 3, 1, 1, 3, 1);
    setPathToNotExist("/dst");
    Map<WorkerInfo, List<MoveCommand>> expected = ImmutableMap.of(JOB_WORKERS.get(1),
        Collections.singletonList(new MoveCommand("/src", "/dst")));
    Assert.assertEquals(expected, assignMoves("/src", "/dst"));
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

    List<MoveCommand> moveCommandsWorker0 = Lists.newArrayList(
        new MoveCommand("/dir/src1", "/dst/src1"), new MoveCommand("/dir/src3", "/dst/src3"));
    List<MoveCommand> moveCommandsWorker2 =
        Lists.newArrayList(new MoveCommand("/dir/src2", "/dst/src2"));
    ImmutableMap<WorkerInfo, List<MoveCommand>> expected =
        ImmutableMap.of(JOB_WORKERS.get(0), moveCommandsWorker0, JOB_WORKERS.get(2),
            moveCommandsWorker2);
    Assert.assertEquals(expected, assignMoves("/dir", "/dst"));
  }

  @Test
  public void moveEmptyDirectory() throws Exception {
    createDirectory("/src");
    createDirectory("/dst");
    setPathToNotExist("/dst/src");
    assignMoves("/src", "/dst/src");
    verify(mMockFileSystem).createDirectory(eq(new AlluxioURI("/dst/src")),
        any(CreateDirectoryPOptions.class));
  }

  @Test
  public void moveNestedEmptyDirectory() throws Exception {
    createDirectory("/src");
    FileInfo nested = createDirectory("/src/nested");
    setChildren("/src", nested);
    createDirectory("/dst");
    setPathToNotExist("/dst/src");
    assignMoves("/src", "/dst/src");
    verify(mMockFileSystem).createDirectory(eq(new AlluxioURI("/dst/src/nested")),
        Matchers.eq(CreateDirectoryPOptions.getDefaultInstance()));
  }

  @Test
  public void moveToSubpath() throws Exception {
    try {
      assignMovesFail("/src", "/src/dst");
    } catch (RuntimeException e) {
      Assert.assertEquals(
          ExceptionMessage.MOVE_CANNOT_BE_TO_SUBDIRECTORY.getMessage("/src", "/src/dst"),
          e.getMessage());
    }
  }

  @Test
  public void moveMissingSource() throws Exception {
    setPathToNotExist("/notExist");
    try {
      assignMovesFail("/notExist", "/dst");
    } catch (FileDoesNotExistException e) {
      Assert.assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/notExist"),
          e.getMessage());
    }
  }

  @Test
  public void moveMissingDestinationParent() throws Exception {
    createDirectory("/src");
    setPathToNotExist("/dst");
    setPathToNotExist("/dst/missing");
    try {
      assignMovesFail("/src", "/dst/missing");
    } catch (Exception e) {
      Assert.assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/dst"), e.getMessage());
    }
  }

  @Test
  public void moveIntoFile() throws Exception {
    createFile("/src");
    createFile("/dst");
    setPathToNotExist("/dst/src");
    try {
      assignMovesFail("/src", "/dst/src");
    } catch (Exception e) {
      Assert.assertEquals(ExceptionMessage.MOVE_TO_FILE_AS_DIRECTORY.getMessage("/dst/src", "/dst"),
          e.getMessage());
    }
  }

  @Test
  public void moveFileToDirectory() throws Exception {
    createFile("/src");
    createDirectory("/dst");
    try {
      assignMovesFail("/src", "/dst");
    } catch (Exception e) {
      Assert.assertEquals(ExceptionMessage.MOVE_FILE_TO_DIRECTORY.getMessage("/src", "/dst"),
          e.getMessage());
    }
  }

  @Test
  public void moveDirectoryToFile() throws Exception {
    createDirectory("/src");
    createFile("/dst");
    try {
      assignMovesFail("/src", "/dst");
    } catch (Exception e) {
      Assert.assertEquals(ExceptionMessage.MOVE_DIRECTORY_TO_FILE.getMessage("/src", "/dst"),
          e.getMessage());
    }
  }

  @Test
  public void moveFileToExistingDestinationWithoutOverwrite() throws Exception {
    createFile("/src");
    createFile("/dst");
    // Test with source being a file.
    try {
      assignMovesFail("/src", "/dst");
    } catch (FileAlreadyExistsException e) {
      Assert.assertEquals(ExceptionMessage.MOVE_NEED_OVERWRITE.getMessage("/dst"), e.getMessage());
    }
  }

  @Test
  public void moveDirectoryToExistingDestinationWithoutOverwrite() throws Exception {
    // Test with the source being a folder.
    createDirectory("/src");
    createDirectory("/dst");
    try {
      assignMovesFail("/src", "/dst");
    } catch (FileAlreadyExistsException e) {
      Assert.assertEquals(ExceptionMessage.MOVE_NEED_OVERWRITE.getMessage("/dst"), e.getMessage());
    }
  }

  @Test
  public void moveFileToExistingDestinationWithOverwrite() throws Exception {
    createFileWithBlocksOnWorkers("/src", 0);
    createFile("/dst");

    Map<WorkerInfo, List<MoveCommand>> expected = ImmutableMap.of(JOB_WORKERS.get(0),
        Collections.singletonList(new MoveCommand("/src", "/dst")));
    // Set overwrite to true.
    Assert.assertEquals(expected, assignMoves(new MoveConfig("/src", "/dst", "THROUGH", true)));
  }

  @Test
  public void moveDirectoryIntoDirectoryWithOverwrite() throws Exception {
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

    List<MoveCommand> moveCommandsWorker1 =
        Lists.newArrayList(new MoveCommand("/src/nested/file2", "/dst/nested/file2"),
            new MoveCommand("/src/nested/moreNested/file3", "/dst/nested/moreNested/file3"));
    List<MoveCommand> moveCommandsWorker2 =
        Lists.newArrayList(new MoveCommand("/src/file1", "/dst/file1"));
    ImmutableMap<WorkerInfo, List<MoveCommand>> expected =
        ImmutableMap.of(JOB_WORKERS.get(1), moveCommandsWorker1, JOB_WORKERS.get(2),
            moveCommandsWorker2);
    Assert.assertEquals(expected, assignMoves(new MoveConfig("/src", "/dst", "THROUGH", true)));
  }

  @Test
  public void moveUncachedFile() throws Exception {
    createFileWithBlocksOnWorkers("/src");
    setPathToNotExist("/dst");
    Assert.assertEquals(1, assignMoves("/src", "/dst").size());
  }

  @Test
  public void moveNoLocalJobWorker() throws Exception {
    createFileWithBlocksOnWorkers("/src", 0);
    setPathToNotExist("/dst");

    Map<WorkerInfo, ArrayList<MoveCommand>> assignments =
        new MoveDefinition(mMockFileSystemContext, mMockFileSystem).selectExecutors(
            new MoveConfig("/src", "/dst", "THROUGH", true), ImmutableList.of(JOB_WORKER_3),
            new JobMasterContext(1, mMockUfsManager));

    Assert.assertEquals(ImmutableMap.of(JOB_WORKER_3,
        new ArrayList<MoveCommand>(Arrays.asList(new MoveCommand("/src", "/dst")))), assignments);
  }

  /**
   * Runs selectExecutors for the move from source to destination.
   */
  private Map<WorkerInfo, ArrayList<MoveCommand>> assignMoves(String source, String destination)
      throws Exception {
    return assignMoves(new MoveConfig(source, destination, "THROUGH", false));
  }

  /**
   * Runs selectExecutors for the move from source to destination with the given writeType and
   * overwrite value.
   */
  private Map<WorkerInfo, ArrayList<MoveCommand>> assignMoves(MoveConfig config) throws Exception {
    return new MoveDefinition(mMockFileSystemContext, mMockFileSystem).selectExecutors(config,
        JOB_WORKERS, new JobMasterContext(1, mMockUfsManager));
  }

  /**
   * Runs selectExecutors with the expectation that it will throw an exception.
   */
  private void assignMovesFail(String source, String destination) throws Exception {
    assignMovesFail(source, destination, "THROUGH", false);
  }

  /**
   * Runs selectExecutors with the expectation that it will throw an exception.
   */
  private void assignMovesFail(String source, String destination, String writeType,
      boolean overwrite) throws Exception {
    Map<WorkerInfo, ArrayList<MoveCommand>> assignment =
        assignMoves(new MoveConfig(source, destination, writeType, overwrite));
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
