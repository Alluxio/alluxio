/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.async;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.FailedToCheckpointException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.thrift.PersistFile;
import alluxio.util.IdUtils;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The default handler for async persistence that schedules the the persistence on the workers that
 * contains all the blocks of a given file, and the handler returns the scheduled request whenever
 * the corresponding worker polls.
 */
public class DefaultAsyncPersistHandler implements AsyncPersistHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final FileSystemMasterView mFileSystemMasterView;

  /** Map from worker to the files to persist on that worker. Used by async persistence service. */
  private final Map<Long, Set<Long>> mWorkerToAsyncPersistFiles;

  /**
   * Constructs a new instance of {@link AsyncPersistHandler}.
   *
   * @param view a view of {@link FileSystemMaster}
   */
  public DefaultAsyncPersistHandler(FileSystemMasterView view) {
    mWorkerToAsyncPersistFiles = Maps.newHashMap();
    mFileSystemMasterView = Preconditions.checkNotNull(view);
  }

  @Override
  public synchronized void scheduleAsyncPersistence(AlluxioURI path)
      throws AlluxioException {
    // find the worker
    long workerId = getWorkerStoringFile(path);

    if (workerId == IdUtils.INVALID_WORKER_ID) {
      throw new FailedToCheckpointException(
          "No worker found to schedule async persistence for file " + path);
    }

    if (!mWorkerToAsyncPersistFiles.containsKey(workerId)) {
      mWorkerToAsyncPersistFiles.put(workerId, Sets.<Long>newHashSet());
    }
    mWorkerToAsyncPersistFiles.get(workerId).add(mFileSystemMasterView.getFileId(path));
  }

  /**
   * Gets a worker where the given file is stored.
   *
   * @param path the path to the file
   * @return the id of the storing worker
   * @throws FileDoesNotExistException when the file does not exist on any worker
   * @throws AccessControlException if permission checking fails
   */
  // TODO(calvin): Propagate the exceptions in certain cases
  private long getWorkerStoringFile(AlluxioURI path)
      throws FileDoesNotExistException, AccessControlException {
    Map<Long, Integer> workerBlockCounts = Maps.newHashMap();
    List<FileBlockInfo> blockInfoList;
    try {
      blockInfoList = mFileSystemMasterView.getFileBlockInfoList(path);

      for (FileBlockInfo fileBlockInfo : blockInfoList) {
        for (BlockLocation blockLocation : fileBlockInfo.getBlockInfo().getLocations()) {
          if (workerBlockCounts.containsKey(blockLocation.getWorkerId())) {
            workerBlockCounts.put(blockLocation.getWorkerId(),
                workerBlockCounts.get(blockLocation.getWorkerId()) + 1);
          } else {
            workerBlockCounts.put(blockLocation.getWorkerId(), 1);
          }

          // all the blocks of a file must be stored on the same worker
          if (workerBlockCounts.get(blockLocation.getWorkerId()) == blockInfoList.size()) {
            return blockLocation.getWorkerId();
          }
        }
      }
    } catch (FileDoesNotExistException e) {
      LOG.error("The file {} to persist does not exist", path);
      return IdUtils.INVALID_WORKER_ID;
    } catch (InvalidPathException e) {
      LOG.error("The file {} to persist is invalid", path);
      return IdUtils.INVALID_WORKER_ID;
    }

    if (workerBlockCounts.size() == 0) {
      LOG.error("The file " + path + " does not exist on any worker");
      return IdUtils.INVALID_WORKER_ID;
    }

    LOG.error("Not all the blocks of file {} stored on the same worker", path);
    return IdUtils.INVALID_WORKER_ID;
  }

  /**
   * Polls the files to send to the given worker for persistence. It also removes files from the
   * worker entry in {@link #mWorkerToAsyncPersistFiles}.
   *
   * @param workerId the worker id
   * @return the list of files
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the path is invalid
   * @throws AccessControlException if permission checking fails
   */
  @Override
  public synchronized List<PersistFile> pollFilesToPersist(long workerId)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    List<PersistFile> filesToPersist = Lists.newArrayList();
    List<Long> fileIdsToPersist = Lists.newArrayList();

    if (!mWorkerToAsyncPersistFiles.containsKey(workerId)) {
      return filesToPersist;
    }

    Set<Long> scheduledFiles = mWorkerToAsyncPersistFiles.get(workerId);
    for (long fileId : scheduledFiles) {
      FileInfo fileInfo = mFileSystemMasterView.getFileInfo(fileId);
      if (fileInfo.isCompleted()) {
        fileIdsToPersist.add(fileId);
        List<Long> blockIds = Lists.newArrayList();
        for (FileBlockInfo fileBlockInfo : mFileSystemMasterView
            .getFileBlockInfoList(mFileSystemMasterView.getPath(fileId))) {
          blockIds.add(fileBlockInfo.getBlockInfo().getBlockId());
        }

        filesToPersist.add(new PersistFile(fileId, blockIds));
      }
    }
    mWorkerToAsyncPersistFiles.get(workerId).removeAll(fileIdsToPersist);
    return filesToPersist;
  }
}
