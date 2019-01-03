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

package alluxio.worker.file;

import alluxio.underfs.UfsManager;
import alluxio.worker.WorkerFactory;
import alluxio.worker.WorkerRegistry;
import alluxio.worker.block.BlockWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link DefaultFileSystemWorker} instance.
 */
@ThreadSafe
public final class FileSystemWorkerFactory implements WorkerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemWorkerFactory.class);

  /**
   * Constructs a new {@link FileSystemWorkerFactory}.
   */
  public FileSystemWorkerFactory() {}

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public FileSystemWorker create(WorkerRegistry registry, UfsManager ufsManager) {
    LOG.info("Creating {} ", FileSystemWorker.class.getName());
    BlockWorker blockWorker = registry.get(BlockWorker.class);
    FileSystemWorker fileSystemWorker = new DefaultFileSystemWorker(blockWorker, ufsManager);
    registry.add(FileSystemWorker.class, fileSystemWorker);
    return fileSystemWorker;
  }
}
