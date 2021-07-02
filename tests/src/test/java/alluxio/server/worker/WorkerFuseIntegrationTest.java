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

package alluxio.server.worker;

import alluxio.client.file.FileSystem;
import alluxio.client.fuse.AbstractFuseIntegrationTest;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

/**
 * Integration tests for worker embedded Fuse application.
 */
public class WorkerFuseIntegrationTest extends AbstractFuseIntegrationTest {
  @Override
  public void configure() {
    ServerConfiguration.set(PropertyKey.WORKER_FUSE_ENABLED, true);
    ServerConfiguration.set(PropertyKey.WORKER_FUSE_MOUNT_POINT, mMountPoint);
    ServerConfiguration.set(PropertyKey.WORKER_FUSE_MOUNT_ALLUXIO_PATH, ALLUXIO_ROOT);
  }

  @Override
  public void mountFuse(FileSystem fileSystem, String mountPoint, String alluxioRoot) {
    // Fuse application is mounted automatically by the worker
  }

  @Override
  public void umountFuse(String mountPath) throws Exception {
    // Fuse application is unmounted automatically when stopping the worker
  }
}
