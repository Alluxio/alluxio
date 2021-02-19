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

package alluxio.worker.block;

import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.fuse.AlluxioFuse;
import alluxio.worker.block.io.LocalBlockWorkerImpl;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import java.io.Closeable;

/**
 * The Fuse manager that responsible for managing the Fuse application lifecycle.
 */
public class FuseManager implements Closeable {
  private final Supplier<FileSystemContext> mFsContextSupplier;
  // TODO(lu) add Fuse unmountable after https://github.com/Alluxio/alluxio/pull/12837

  /**
   * Constructs a new {@link FuseManager}.
   *
   * @param blockWorker the block worekr
   */
  public FuseManager(BlockWorker blockWorker) {
    LocalBlockWorkerImpl localBlockWorker = new LocalBlockWorkerImpl(blockWorker);
    // TODO(lu) modify FileSystemContext to accept LocalWorkerImpl
    mFsContextSupplier = Suppliers.memoize(()
        -> FileSystemContext.create(ServerConfiguration.global(), localBlockWorker));
  }

  /**
   * Starts mounting the internal Fuse applications.
   */
  public void start() {
    String fuseMount = ServerConfiguration.get(PropertyKey.WORKER_FUSE_MOUNT_POINT);
    if (fuseMount.isEmpty()) {
      return;
    }

    // TODO(lu) change the call AlluxioFuse.launchFuse function after https://github.com/Alluxio/alluxio/pull/12837
    AlluxioFuse.main(new String[]{});
  }

  @Override
  public void close() {
    // TODO(lu) Unmount the mounted Fuse after https://github.com/Alluxio/alluxio/pull/12837
  }
}
