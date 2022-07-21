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
import alluxio.jnifuse.FuseFileSystem;
import alluxio.jnifuse.struct.FuseContext;

import com.google.common.base.Preconditions;

import java.util.Optional;

/**
 * The system user group authentication policy that always set the user group
 * to the actual end user.
 * Note that this may downgrade the performance of creating file/directory.
 */
public final class SystemUserGroupAuthPolicy extends LaunchUserGroupAuthPolicy {

  /**
   * Creates a new system auth policy.
   *
   * @param fileSystem file system
   * @param fuseFsOpts fuse options
   * @param fuseFileSystem fuse file system
   * @return system auth policy
   */
  public static SystemUserGroupAuthPolicy create(FileSystem fileSystem,
      AlluxioFuseFileSystemOpts fuseFsOpts, Optional<FuseFileSystem> fuseFileSystem) {
    return new SystemUserGroupAuthPolicy(fileSystem, fuseFsOpts, fuseFileSystem);
  }

  /**
   * @param fileSystem     the Alluxio file system
   * @param fuseFsOpts     the options for AlluxioFuse filesystem
   * @param fuseFileSystem AbstractFuseFileSystem
   */
  private SystemUserGroupAuthPolicy(FileSystem fileSystem, AlluxioFuseFileSystemOpts fuseFsOpts,
      Optional<FuseFileSystem> fuseFileSystem) {
    super(fileSystem, fuseFsOpts, fuseFileSystem);
    Preconditions.checkArgument(fuseFileSystem.isPresent());
  }

  @Override
  public void setUserGroupIfNeeded(AlluxioURI uri) {
    FuseContext fc = mFuseFileSystem.get().getContext();
    setUserGroup(uri, fc.uid.get(), fc.gid.get());
  }

  @Override
  public Optional<Long> getUid(String owner) {
    return AlluxioFuseUtils.getUid(owner);
  }

  @Override
  public Optional<Long> getGid(String group) {
    return AlluxioFuseUtils.getGidFromGroupName(group);
  }
}
