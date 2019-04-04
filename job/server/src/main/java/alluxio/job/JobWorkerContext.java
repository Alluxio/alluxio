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

package alluxio.job;

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.underfs.UfsManager;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The context of worker-side resources and information.
 */
@ThreadSafe
public final class JobWorkerContext {
  private final FileSystem mFileSystem;
  private final FileSystemContext mFsContext;
  private final long mJobId;
  private final int mTaskId;
  private final UfsManager mUfsManager;

  /**
   * Creates a new instance of {@link JobWorkerContext}.
   *
   * @param filesystem the filesystem client that is used when running jobs
   * @param fsContext the filesystem client's underlying context
   * @param jobId the job id
   * @param taskId the task id
   * @param ufsManager the UFS manager
   */
  public JobWorkerContext(FileSystem filesystem, FileSystemContext fsContext, long jobId,
      int taskId, UfsManager ufsManager) {
    mFsContext = fsContext;
    mFileSystem = filesystem;
    mJobId = jobId;
    mTaskId = taskId;
    mUfsManager = ufsManager;
  }

  /**
   * @return the {@link FileSystem} client
   */
  public FileSystem getFileSystem() {
    return mFileSystem;
  }

  /**
   * @return the FileSystemContext for the client
   */
  public FileSystemContext getFsContext() {
    return mFsContext;
  }

  /**
   * @return the job id
   */
  public long getJobId() {
    return mJobId;
  }

  /**
   * @return the task id
   */
  public int getTaskId() {
    return mTaskId;
  }

  /**
   * @return the UFS manager
   */
  public UfsManager getUfsManager() {
    return mUfsManager;
  }
}
