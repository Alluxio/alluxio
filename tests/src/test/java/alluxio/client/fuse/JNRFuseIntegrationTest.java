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
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.fuse.AlluxioFuseFileSystemOpts;
import alluxio.fuse.AlluxioJnrFuseFileSystem;

import java.nio.file.Paths;

/**
 * Integration tests for JNR-FUSE based {@link AlluxioJnrFuseFileSystem}.
 */
public class JNRFuseIntegrationTest extends AbstractFuseIntegrationTest {
  private AlluxioJnrFuseFileSystem mFuseFileSystem;

  @Override
  public void configure() {
    Configuration.set(PropertyKey.FUSE_JNIFUSE_ENABLED, false);
  }

  @Override
  public void mountFuse(FileSystemContext context,
      FileSystem fileSystem, String mountPoint, String alluxioRoot) {
    Configuration.set(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH, alluxioRoot);
    Configuration.set(PropertyKey.FUSE_MOUNT_POINT, mountPoint);
    Configuration.set(PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED, true);
    AlluxioConfiguration conf = Configuration.global();
    AlluxioFuseFileSystemOpts fuseFsOpts = AlluxioFuseFileSystemOpts.create(conf);
    mFuseFileSystem = new AlluxioJnrFuseFileSystem(fileSystem, fuseFsOpts);
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
