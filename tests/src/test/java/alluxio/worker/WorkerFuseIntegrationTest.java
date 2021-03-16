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

package alluxio.worker;

import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.LocalAlluxioCluster;
import alluxio.util.ShellUtils;

/**
 * Integration tests for worker embedded Fuse application.
 */
public class WorkerFuseIntegrationTest extends AbstractFuseIntegrationTest {
  @Override
  public LocalAlluxioCluster createLocalAlluxioCluster(String clusterName,
      int blockSize, String mountPath, String alluxioRoot) throws Exception {
    LocalAlluxioCluster localAlluxioCluster = new LocalAlluxioCluster();
    localAlluxioCluster.initConfiguration(clusterName);
    // Overwrite the test configuration with test specific parameters
    ServerConfiguration.set(PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED, true);
    ServerConfiguration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, blockSize);
    ServerConfiguration.set(PropertyKey.WORKER_FUSE_ENABLED, true);
    ServerConfiguration.set(PropertyKey.WORKER_FUSE_MOUNT_POINT, mountPath);
    ServerConfiguration.set(PropertyKey.WORKER_FUSE_MOUNT_ALLUXIO_PATH, alluxioRoot);
    ServerConfiguration.global().validate();
    localAlluxioCluster.start();
    return localAlluxioCluster;
  }

  @Override
  public void mountFuse(FileSystem fileSystem, String mountPoint, String alluxioRoot) {
    // mo need to manually mount fuse application
  }

  @Override
  public void umountFuse(String mountPath) throws Exception {
    // shell command umount is not needed if Fuse application can be umounted in worker.stop().
    // TODO(lu) add umount in worker.stop(), otherwise all the Fuse applications
    // will remain running for the entire test and may potentially prevent new Fuse applications
    // from coming up due to Fuse device number limitations
    ShellUtils.execCommand("umount", mountPath);
  }
}
