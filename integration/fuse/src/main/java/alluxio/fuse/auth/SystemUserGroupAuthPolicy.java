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

package alluxio.fuse.auth;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.fuse.AlluxioFuseFileSystemOpts;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.jnifuse.AbstractFuseFileSystem;
import alluxio.jnifuse.struct.FuseContext;

/**
 * The system user group authentication policy that always set the user group
 * to the actual end user.
 * Note that this may downgrade the performance of creating file/directory.
 */
public final class SystemUserGroupAuthPolicy extends LaunchUserGroupAuthPolicy {
  /**
   * @param fileSystem     the Alluxio file system
   * @param fuseFsOpts     the options for AlluxioFuse filesystem
   * @param fuseFileSystem AbstractFuseFileSystem
   */
  public SystemUserGroupAuthPolicy(FileSystem fileSystem, AlluxioFuseFileSystemOpts fuseFsOpts,
      AbstractFuseFileSystem fuseFileSystem) {
    super(fileSystem, fuseFsOpts, fuseFileSystem);
  }

  @Override
  public void setUserGroupIfNeeded(AlluxioURI uri) {
    FuseContext fc = mFuseFileSystem.getContext();
    setUserGroup(uri, fc.uid.get(), fc.gid.get());
  }

  @Override
  public long getUid(String owner) {
    return AlluxioFuseUtils.getUid(owner);
  }

  @Override
  public long getGid(String group) {
    return AlluxioFuseUtils.getGidFromGroupName(group);
  }
}
