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

package alluxio.master.file;

import alluxio.heartbeat.HeartbeatExecutor;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Under file system periodic clean.
 */
@NotThreadSafe
final class UfsCleaner implements HeartbeatExecutor {
  private final FileSystemMaster mFileSystemMaster;

  /**
   * Constructs a new {@link UfsCleaner}.
   */
  public UfsCleaner(FileSystemMaster fileSystemMaster) {
    mFileSystemMaster = fileSystemMaster;
  }

  @Override
  public void heartbeat() {
    mFileSystemMaster.cleanupUfs();
  }

  @Override
  public void close() {
    // Nothing to clean up
  }
}
