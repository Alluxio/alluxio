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
import alluxio.fuse.AlluxioFuseFileSystem;
import alluxio.fuse.FuseMountOptions;

import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * Integration tests for JNR-FUSE based {@link AlluxioFuseFileSystem}.
 */
public class JNRFuseIntegrationTest extends AbstractFuseIntegrationTest {
  private AlluxioFuseFileSystem mFuseFileSystem;

  @Override
  public void configure() {
    ServerConfiguration.set(PropertyKey.FUSE_JNIFUSE_ENABLED, false);
  }

  @Override
  public void mountFuse(FileSystem fileSystem, String mountPoint, String alluxioRoot) {
    FuseMountOptions options =
        new FuseMountOptions(mountPoint, alluxioRoot, false, new ArrayList<>());
    mFuseFileSystem = new AlluxioFuseFileSystem(fileSystem, options, ServerConfiguration.global());
    mFuseFileSystem.mount(Paths.get(mountPoint), false, false, new String[] {"-odirect_io"});
  }

  @Override
  public void umountFuse(String mountPath) throws Exception {
    mFuseFileSystem.umount();
  }
}
