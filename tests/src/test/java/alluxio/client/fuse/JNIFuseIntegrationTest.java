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
import alluxio.fuse.AlluxioFuseOptions;
import alluxio.fuse.AlluxioJniFuseFileSystem;

import org.junit.Ignore;

import java.util.ArrayList;

/**
 * Integration tests for JNR-FUSE based {@link AlluxioJniFuseFileSystem}.
 */
public class JNIFuseIntegrationTest extends AbstractFuseIntegrationTest {
  private AlluxioJniFuseFileSystem mFuseFileSystem;

  @Override
  public void configureAlluxioCluster() {
    super.configureAlluxioCluster();
    // Overwrite the test configuration with test specific parameters
    ServerConfiguration.set(PropertyKey.FUSE_JNIFUSE_ENABLED, true);
  }

  @Override
  public void mountFuse(FileSystem fileSystem, String mountPoint, String alluxioRoot) {
    AlluxioFuseOptions options =
        new AlluxioFuseOptions(mountPoint, alluxioRoot, false, new ArrayList<>());
    mFuseFileSystem =
        new AlluxioJniFuseFileSystem(fileSystem, options, ServerConfiguration.global());
    mFuseFileSystem.mount(false, false, new String[] {});
  }

  @Override
  public void umountFuse(String mountPath) throws Exception {
    mFuseFileSystem.umount();
  }

  @Ignore
  @Override
  public void touchAndLs() throws Exception {
    // TODO(lu) Enable the test after https://github.com/Alluxio/alluxio/issues/13090 solved
  }
}
