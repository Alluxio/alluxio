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

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemClientOptions;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.util.JobUtils;
import alluxio.job.util.SerializableVoid;
import alluxio.util.io.PathUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.ConcurrentMap;

/**
 * A job that moves a source path to a destination path.
 *
 * If the destination exists, the source and destination must either both be files or both be
 * directories, and the overwrite configuration option must be set. If the destination does not
 * exist, its parent must be a directory and the destination will be created by the move command.
 *
 * If moving a directory to an existing directory causes files to conflict, the moved files will
 * replace the existing files.
 *
 * Unlike Unix mv, the source will not be nested inside the destination when the destination is a
 * directory. This makes it so that the move job is idempotent when the overwrite flag is set.
 *
 * Suppose we have this directory structure, where a to e are directories and f1 to f3 are files:
 *
 * ├── a
 * │   ├── e
 * │   │   └── f2
 * │   └── f1
 * └── b
 *     └── d
 *     └── e
 *         └── f3
 *
 * Moving a to b will result in
 *
 * ├── b
 *     ├── d
 *     ├── e
 *     |   └── f2
 *     └── f1
 */
public final class MoveDefinition
    extends AbstractVoidJobDefinition<MoveConfig, ArrayList<MoveCommand>> {
  private static final Logger LOG = LoggerFactory.getLogger(MoveDefinition.class);
  private final FileSystemContext mFileSystemContext;
  private final FileSystem mFileSystem;
  private final Random mRandom = new Random();

  /**
   * Constructs a new {@link MoveDefinition}.
   */
  public MoveDefinition() {
    mFileSystemContext = FileSystemContext.get();
    mFileSystem = BaseFileSystem.get(FileSystemContext.get());
  }

  /**
   * Constructs a new {@link MoveDefinition} with FileSystem context and instance.
   *
   * @param context file system context
   * @param fileSystem file system client
   */
  public MoveDefinition(FileSystemContext context, FileSystem fileSystem) {
    mFileSystemContext = context;
    mFileSystem = fileSystem;
  }

  private void checkMoveValid(MoveConfig config) throws Exception {
    AlluxioURI source = new AlluxioURI(config.getSource());
    AlluxioURI destination = new AlluxioURI(config.getDestination());
    // The source cannot be a prefix of the destination - that would be moving a path inside itself.
    if (PathUtils.hasPrefix(destination.toString(), source.toString())) {
      throw new RuntimeException(ExceptionMessage.MOVE_CANNOT_BE_TO_SUBDIRECTORY.getMessage(source,
          config.getDestination()));
    }

    // This will throw an appropriate exception if the source does not exist.
    boolean sourceIsDirectory = mFileSystem.getStatus(source).isFolder();
    try {
      URIStatus destinationStatus = mFileSystem.getStatus(destination);
      // Handle the case where the destination exists.
      boolean destinationIsDirectory = destinationStatus.isFolder();
      if (sourceIsDirectory && !destinationIsDirectory) {
        throw new RuntimeException(
            ExceptionMessage.MOVE_DIRECTORY_TO_FILE.getMessage(source, destination));
      } else if (!sourceIsDirectory && destinationIsDirectory) {
        throw new RuntimeException(
            ExceptionMessage.MOVE_FILE_TO_DIRECTORY.getMessage(source, destination));
      }
      if (!config.isOverwrite()) {
        throw new FileAlreadyExistsException(
            ExceptionMessage.MOVE_NEED_OVERWRITE.getMessage(destination));
      }
    } catch (FileDoesNotExistException e) {
      // Handle the case where the destination does not exist.
      // This will throw an appropriate exception if the destination's parent does not exist.
      URIStatus destinationParentStatus = mFileSystem.getStatus(destination.getParent());
      if (!destinationParentStatus.isFolder()) {
        throw new RuntimeException(ExceptionMessage.MOVE_TO_FILE_AS_DIRECTORY
            .getMessage(destination, destination.getParent()));
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * Assigns each worker to move whichever files it has the most blocks for. If no worker has blocks
   * for a file, a random worker is chosen.
   */
  @Override
  public Map<WorkerInfo, ArrayList<MoveCommand>> selectExecutors(MoveConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    AlluxioURI source = new AlluxioURI(config.getSource());
    AlluxioURI destination = new AlluxioURI(config.getDestination());
    if (source.equals(destination)) {
      return new HashMap<WorkerInfo, ArrayList<MoveCommand>>();
    }
    checkMoveValid(config);

    List<BlockWorkerInfo> alluxioWorkerInfoList =
        AlluxioBlockStore.create(mFileSystemContext).getAllWorkers();
    Preconditions.checkState(!jobWorkerInfoList.isEmpty(), "No workers are available");

    List<URIStatus> allPathStatuses = getPathStatuses(source);
    ConcurrentMap<WorkerInfo, ArrayList<MoveCommand>> assignments = Maps.newConcurrentMap();
    ConcurrentMap<String, WorkerInfo> hostnameToWorker = Maps.newConcurrentMap();
    for (WorkerInfo workerInfo : jobWorkerInfoList) {
      hostnameToWorker.put(workerInfo.getAddress().getHost(), workerInfo);
    }
    List<String> keys = new ArrayList<>();
    keys.addAll(hostnameToWorker.keySet());
    // Assign each file to the worker with the most block locality.
    for (URIStatus status : allPathStatuses) {
      if (status.isFolder()) {
        moveDirectory(status.getPath(), source.getPath(), destination.getPath());
      } else {
        WorkerInfo bestJobWorker = getBestJobWorker(status, alluxioWorkerInfoList,
            jobWorkerInfoList, hostnameToWorker);
        String destinationPath =
            computeTargetPath(status.getPath(), source.getPath(), destination.getPath());
        assignments.putIfAbsent(bestJobWorker, Lists.<MoveCommand>newArrayList());
        assignments.get(bestJobWorker).add(new MoveCommand(status.getPath(), destinationPath));
      }
    }
    return assignments;
  }

  private WorkerInfo getBestJobWorker(URIStatus status, List<BlockWorkerInfo> alluxioWorkerInfoList,
      List<WorkerInfo> jobWorkerInfoList, Map<String, WorkerInfo> hostnameToJobWorker) {
    BlockWorkerInfo bestWorker =
        JobUtils.getWorkerWithMostBlocks(alluxioWorkerInfoList, status.getFileBlockInfos());
    if (bestWorker == null) {
      // Nobody has blocks, choose a random worker.
      bestWorker = alluxioWorkerInfoList.get(mRandom.nextInt(jobWorkerInfoList.size()));
    }
    // Map the best Alluxio worker to a job worker.
    WorkerInfo worker = hostnameToJobWorker.get(bestWorker.getNetAddress().getHost());
    if (worker == null) {
      return jobWorkerInfoList.get(new Random().nextInt(jobWorkerInfoList.size()));
    }
    return worker;
  }

  /**
   * Computes the path that the given path should end up at when source is moved to destination.
   *
   * @param path a path to move which must be a descendent path of the source path, e.g. /src/file
   * @param source the base source path being moved, e.g. /src
   * @param destination the path to move to, e.g. /dst/src
   * @return the path which file should be moved to, e.g. /dst/src/file
   */
  private static String computeTargetPath(String path, String source, String destination)
      throws Exception {
    String relativePath = PathUtils.subtractPaths(path, source);
    return PathUtils.concatPath(destination, relativePath);
  }

  /**
   * @param path the path of the directory to move; it must be a subpath of source
   * @param source the base source path being moved
   * @param destination the destination path
   */
  private void moveDirectory(String path, String source, String destination) throws Exception {
    String newDir = computeTargetPath(path, source, destination);
    mFileSystem.createDirectory(new AlluxioURI(newDir),
        FileSystemClientOptions.getCreateDirectoryOptions());
  }

  /**
   * Returns {@link URIStatus} for all paths under the specified path, including the path itself.
   *
   * The statuses will be listed in the order they are visited by depth-first search.
   *
   * @param path the target path
   * @return a list of the {@link URIStatus} for all paths under the given path
   * @throws Exception if an exception occurs
   */
  private List<URIStatus> getPathStatuses(AlluxioURI path) throws Exception {
    // Depth-first search to to find all files under path.
    Stack<AlluxioURI> pathsToConsider = new Stack<>();
    pathsToConsider.add(path);
    List<URIStatus> allStatuses = Lists.newArrayList();
    while (!pathsToConsider.isEmpty()) {
      AlluxioURI nextPath = pathsToConsider.pop();
      URIStatus status = mFileSystem.getStatus(nextPath);
      allStatuses.add(status);
      if (status.isFolder()) {
        List<URIStatus> childStatuses = mFileSystem.listStatus(nextPath);
        for (URIStatus childStatus : childStatuses) {
          if (childStatus.isFolder()) {
            pathsToConsider.push(new AlluxioURI(childStatus.getPath()));
          } else {
            allStatuses.add(childStatus);
          }
        }
      }
    }
    return ImmutableList.copyOf(allStatuses);
  }

  /**
   * {@inheritDoc}
   *
   * Moves the file specified in the config to the configured path. If the destination path is a
   * directory, the file is moved inside that directory.
   */
  @Override
  public SerializableVoid runTask(MoveConfig config, ArrayList<MoveCommand> commands,
      JobWorkerContext jobWorkerContext) throws Exception {
    WriteType writeType = config.getWriteType() == null
        ? Configuration.getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class)
        : WriteType.valueOf(config.getWriteType());
    for (MoveCommand command : commands) {
      move(command, writeType.toProto(), mFileSystem);
    }
    // Try to delete the source directory if it is empty.
    if (!hasFiles(new AlluxioURI(config.getSource()), mFileSystem)) {
      try {
        LOG.debug("Deleting {}", config.getSource());
        mFileSystem.delete(new AlluxioURI(config.getSource()),
            FileSystemClientOptions.getDeleteOptions().toBuilder().setRecursive(true).build());
      } catch (FileDoesNotExistException e) {
        // It's already deleted, possibly by another worker.
      }
    }
    return null;
  }

  /**
   * @param command the move command to execute
   * @param writeType the write type to use for the moved file
   * @param fileSystem the Alluxio file system
   */
  private static void move(MoveCommand command, WritePType writeType, FileSystem fileSystem)
      throws Exception {
    String source = command.getSource();
    String destination = command.getDestination();
    LOG.debug("Moving {} to {}", source, destination);

    CreateFilePOptions createOptions =
        FileSystemClientOptions.getCreateFileOptions().toBuilder().setWriteType(writeType).build();

    try (FileOutStream out = fileSystem.createFile(new AlluxioURI(destination), createOptions)) {
      try (FileInStream in = fileSystem.openFile(new AlluxioURI(source))) {
        IOUtils.copy(in, out);
      } catch (Throwable t) {
        try {
          out.cancel();
        } catch (Throwable t2) {
          t.addSuppressed(t2);
        }
        throw t;
      }
    }
    fileSystem.delete(new AlluxioURI(source));
  }

  /**
   * @param source an Alluxio URI
   * @param fileSystem the Alluxio file system
   * @return whether the URI is a file or a directory which contains files (including recursively)
   * @throws Exception if an unexpected exception occurs
   */
  private static boolean hasFiles(AlluxioURI source, FileSystem fileSystem) throws Exception {
    Stack<AlluxioURI> dirsToCheck = new Stack<>();
    dirsToCheck.add(source);
    while (!dirsToCheck.isEmpty()) {
      try {
        for (URIStatus status : fileSystem.listStatus(dirsToCheck.pop())) {
          if (!status.isFolder()) {
            return true;
          }
          dirsToCheck.push(new AlluxioURI(status.getPath()));
        }
      } catch (FileDoesNotExistException e) {
        // This probably means another worker has deleted the directory already, so we can probably
        // return false here. To be safe though, we will fall through and complete the search.
      }
    }
    return false;
  }

  @Override
  public Class<MoveConfig> getJobConfigClass() {
    return MoveConfig.class;
  }
}
