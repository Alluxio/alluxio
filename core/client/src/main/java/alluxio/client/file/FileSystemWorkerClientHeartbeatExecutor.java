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

package alluxio.client.file;

import alluxio.heartbeat.HeartbeatExecutor;

/**
 * Session client that sends periodic heartbeats to the file system worker in order to preserve
 * its state on the worker.
 */
public class FileSystemWorkerClientHeartbeatExecutor implements HeartbeatExecutor {
  /** the file system worker client to heartbeat for. */
  private final FileSystemWorkerClient mFileSystemWorkerClient;

  /**
   * Constructor for a file system worker client heartbeat executor.
   *
   * @param client the file system worker client to heartbeat for
   */
  public FileSystemWorkerClientHeartbeatExecutor(FileSystemWorkerClient client) {
    mFileSystemWorkerClient = client;
  }

  @Override
  public void heartbeat() {
    mFileSystemWorkerClient.periodicHeartbeat();
  }

  @Override
  public void close() {
    // Not responsible for cleaning up the client
  }
}
