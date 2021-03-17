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

package alluxio.client.fuse;

import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.fuse.AlluxioJniFuseFileSystem;
import alluxio.fuse.FuseMountOptions;
import alluxio.master.LocalAlluxioCluster;
import alluxio.util.ShellUtils;

import java.util.ArrayList;

/**
 * Integration tests for {@link alluxio.fuse.AlluxioJniFuseFileSystem}.
 */
public class JNIFuseIntegrationTest extends AbstractFuseIntegrationTest {
  private AlluxioJniFuseFileSystem mFuseFileSystem;

  @Override
  public LocalAlluxioCluster createLocalAlluxioCluster(String clusterName,
      int blockSize, String mountPath, String alluxioRoot) throws Exception {
    LocalAlluxioCluster localAlluxioCluster = new LocalAlluxioCluster();
    localAlluxioCluster.initConfiguration(clusterName);
    // Overwrite the test configuration with test specific parameters
    ServerConfiguration.set(PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED, true);
    ServerConfiguration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, blockSize);
    ServerConfiguration.set(PropertyKey.FUSE_JNIFUSE_ENABLED, true);
    ServerConfiguration.global().validate();
    localAlluxioCluster.start();
    return localAlluxioCluster;
  }

  @Override
  public void mountFuse(FileSystem fileSystem, String mountPoint, String alluxioRoot) {
    FuseMountOptions options = new FuseMountOptions(mountPoint,
        alluxioRoot, false, new ArrayList<>());
    mFuseFileSystem = new AlluxioJniFuseFileSystem(fileSystem, options,
        ServerConfiguration.global());
    mFuseFileSystem.mount(false, false, new String[]{});
  }

  @Override
  public void umountFuse(String mountPath) throws Exception {
    mFuseFileSystem.umount();
    if (fuseMounted()) {
      ShellUtils.execCommand("umount", mountPath);
    }
  }
}
