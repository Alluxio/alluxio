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
import alluxio.fuse.FuseMountOptions;
import alluxio.worker.block.io.LocalBlockWorkerImpl;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;

/**
 * The Fuse manager that is responsible for managing the Fuse application lifecycle.
 */
public class FuseManager implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FuseManager.class);
  private final LocalBlockWorkerImpl mLocalBlockWorker;
  private final Supplier<FileSystemContext> mFsContextSupplier;

  /**
   * Constructs a new {@link FuseManager}.
   *
   * @param blockWorker the block worekr
   */
  public FuseManager(BlockWorker blockWorker) {
    mLocalBlockWorker = new LocalBlockWorkerImpl(blockWorker);
    mFsContextSupplier = Suppliers.memoize(()
        -> FileSystemContext.create(ServerConfiguration.global(), mLocalBlockWorker));
  }

  /**
   * Starts mounting the internal Fuse applications.
   */
  public void start() {
    String fuseMount = ServerConfiguration.get(PropertyKey.WORKER_FUSE_MOUNT_POINT);
    if (fuseMount.isEmpty()) {
      return;
    }
    String alluxioPath = ServerConfiguration.get(PropertyKey.WORKER_FUSE_MOUNT_ALLUXIO_PATH);
    Preconditions.checkArgument(!alluxioPath.isEmpty(),
        String.format("%s should not be an empty string",
            PropertyKey.WORKER_FUSE_MOUNT_ALLUXIO_PATH));
    LOG.info("Start mounting Fuse application.");
    List<String> fuseOptions = AlluxioFuse.parseFuseOptions(
        ServerConfiguration.get(PropertyKey.WORKER_FUSE_MOUNT_ALLUXIO_PATH).split(","),
        ServerConfiguration.global());
    FuseMountOptions options = new FuseMountOptions(fuseMount, alluxioPath,
        ServerConfiguration.getBoolean(PropertyKey.FUSE_DEBUG_ENABLED), fuseOptions);
    try {
      AlluxioFuse.launchFuse(mFsContextSupplier.get(), options, false);
    } catch (Throwable throwable) {
      // TODO(lu) test what if fuse application error out in the middle, will it affect worker
      // TODO(lu) what if the fuse is already mounted and didn't be closed properly in the previous worker shutdown
      LOG.error("Failed to launch worker internal Fuse application", throwable);
    }
  }

  @Override
  public void close() {
    // TODO(lu) we rely on JVM shutdown hook to close the fuse application
  }
}
