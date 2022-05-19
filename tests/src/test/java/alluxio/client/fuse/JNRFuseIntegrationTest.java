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
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.fuse.AlluxioFuseFileSystem;
import alluxio.fuse.AlluxioFuseFileSystemOpts;

import com.google.common.collect.ImmutableList;

import java.nio.file.Paths;

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
  public void mountFuse(FileSystemContext context,
      FileSystem fileSystem, String mountPoint, String alluxioRoot) {
    InstancedConfiguration conf = ServerConfiguration.global();
    AlluxioFuseFileSystemOpts fuseFsOpts =
        AlluxioFuseFileSystemOpts.create(alluxioRoot, mountPoint, ImmutableList.of(), false);
    mFuseFileSystem = new AlluxioFuseFileSystem(fileSystem, fuseFsOpts, conf);
    mFuseFileSystem.mount(Paths.get(mountPoint), false, false, new String[] {"-odirect_io"});
  }

  @Override
  public void beforeStop() throws Exception {
    try {
      mFuseFileSystem.umount();
    } catch (Exception e) {
      // will try umounting from shell
    }
    umountFromShellIfMounted();
  }

  @Override
  public void afterStop() throws Exception {
    // noop
  }
}
