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

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.fuse.AlluxioFuse;
import alluxio.fuse.FuseMountConfig;
import alluxio.fuse.FuseUmountable;

import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * The Fuse manager that is responsible for managing the Fuse application lifecycle.
 */
public class FuseManager implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FuseManager.class);
  private final FileSystemContext mFsContext;
  /** Use to umount Fuse application during stop. */
  private FuseUmountable mFuseUmountable;
  /** Use to close resources during stop. */
  private final Closer mResourceCloser;

  /**
   * Constructs a new {@link FuseManager}.
   *
   * @param fsContext fs context
   */
  public FuseManager(FileSystemContext fsContext) {
    mFsContext = fsContext;
    mResourceCloser = Closer.create();
  }

  /**
   * Starts mounting the internal Fuse applications.
   */
  public void start() {
    AlluxioConfiguration conf = ServerConfiguration.global();
    try {
      FuseMountConfig config = FuseMountConfig.create(conf);
      // TODO(lu) consider launching fuse in a separate thread as blocking operation
      // so that we can know about the fuse application status
      FileSystem fileSystem = mResourceCloser.register(FileSystem.Factory.create(mFsContext));
      mFuseUmountable = AlluxioFuse.launchFuse(mFsContext, fileSystem, conf, config, false);
    } catch (Throwable throwable) {
      // TODO(lu) for already mounted application, unmount first and then remount
      LOG.error("Failed to launch worker internal Fuse application", throwable);
    }
  }

  @Override
  public void close() throws IOException {
    if (mFuseUmountable != null) {
      try {
        mFuseUmountable.umount(true);
      } catch (Throwable throwable) {
        LOG.error("Failed to umount worker internal Fuse application", throwable);
      }
    }
    mResourceCloser.close();
  }
}
