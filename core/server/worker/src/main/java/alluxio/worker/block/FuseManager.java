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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.fuse.AlluxioFuse;
import alluxio.fuse.FuseMountOptions;
import alluxio.fuse.FuseUmountable;

import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * The Fuse manager that is responsible for managing the Fuse application lifecycle.
 */
public class FuseManager implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FuseManager.class);
  private static final String FUSE_OPTION_SEPARATOR = ",";
  private final FileSystemContext mFsContext;
  /** Use to umount Fuse application during stop. */
  private FuseUmountable mFuseUmountable;
  /** Use to close resources during stop. */
  private Closer mResourceCloser;

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
    if (!conf.isSet(PropertyKey.WORKER_FUSE_MOUNT_POINT)
        || conf.get(PropertyKey.WORKER_FUSE_MOUNT_POINT).isEmpty()) {
      LOG.error("Failed to launch worker internal Fuse application. {} should be set.",
          PropertyKey.WORKER_FUSE_MOUNT_POINT);
      return;
    }
    if (!conf.isSet(PropertyKey.WORKER_FUSE_MOUNT_ALLUXIO_PATH)
        || conf.get(PropertyKey.WORKER_FUSE_MOUNT_ALLUXIO_PATH).isEmpty()) {
      LOG.error("Failed to launch worker internal Fuse application. {} should be set.",
          PropertyKey.WORKER_FUSE_MOUNT_ALLUXIO_PATH.getName());
      return;
    }
    String fuseMount = conf.get(PropertyKey.WORKER_FUSE_MOUNT_POINT);
    String alluxioPath = conf.get(PropertyKey.WORKER_FUSE_MOUNT_ALLUXIO_PATH);
    // TODO(lu) check if the given fuse mount point exists
    // create the folder if it does not exist
    try {
      String[] fuseOptsSeparated = new String[0];
      if (conf.isSet(PropertyKey.WORKER_FUSE_MOUNT_OPTIONS)) {
        String fuseOptsString = conf.get(PropertyKey.WORKER_FUSE_MOUNT_OPTIONS);
        if (!fuseOptsString.isEmpty()) {
          fuseOptsSeparated = fuseOptsString.split(FUSE_OPTION_SEPARATOR);
        }
      }
      List<String> fuseOptions = AlluxioFuse.parseFuseOptions(fuseOptsSeparated, conf);
      FuseMountOptions options = new FuseMountOptions(fuseMount, alluxioPath,
          conf.getBoolean(PropertyKey.FUSE_DEBUG_ENABLED), fuseOptions);
      // TODO(lu) consider launching fuse in a separate thread as blocking operation
      // so that we can know about the fuse application status
      FileSystem fileSystem = mResourceCloser.register(FileSystem.Factory.create(mFsContext));
      mFuseUmountable = AlluxioFuse.launchFuse(fileSystem, conf, options, false);
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
