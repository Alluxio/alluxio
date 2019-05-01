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
 * The context is used by job to access master-side resources and information.
 */
@ThreadSafe
public class JobServerContext {

  /** The Alluxio filesystem client that all threads should use in this process. */
  private final FileSystem mFileSystem;
  /** The underlying context for the filesystem client. */
  private final FileSystemContext mFsContext;
  /** The manager for all ufs. */
  private final UfsManager mUfsManager;

  /**
   * Creates a new instance of {@link JobServerContext}.
   *
   * @param filesystem the Alluxio client used to contact the master
   * @param fsContext the filesystem client's underlying context
   * @param ufsManager the UFS manager
   */
  public JobServerContext(FileSystem filesystem, FileSystemContext fsContext,
      UfsManager ufsManager) {
    mFsContext = fsContext;
    mFileSystem = filesystem;
    mUfsManager = ufsManager;
  }

  JobServerContext(JobServerContext ctx) {
    mFsContext = ctx.mFsContext;
    mFileSystem = ctx.mFileSystem;
    mUfsManager = ctx.mUfsManager;
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
   * @return the UFS manager
   */
  public UfsManager getUfsManager() {
    return mUfsManager;
  }
}
