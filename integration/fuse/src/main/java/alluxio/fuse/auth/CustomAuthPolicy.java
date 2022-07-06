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
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.fuse.AlluxioFuseFileSystemOpts;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.grpc.SetAttributePOptions;
import alluxio.jnifuse.AbstractFuseFileSystem;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A Fuse authentication policy supports user-defined user and group.
 */
public class CustomAuthPolicy extends FuseUserGroupAuthPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(CustomAuthPolicy.class);
  private final long mUid;
  private final long mGid;
  private final SetAttributePOptions mSetAttributeOptions;

  /**
   * @param fileSystem     the Alluxio file system
   * @param fuseFsOpts     the options for AlluxioFuse filesystem
   * @param fuseFileSystem the FuseFileSystem
   */
  public CustomAuthPolicy(FileSystem fileSystem, AlluxioFuseFileSystemOpts fuseFsOpts,
      AbstractFuseFileSystem fuseFileSystem) {
    super(fileSystem, fuseFsOpts, fuseFileSystem);
    String className = getClass().getName();
    Preconditions.checkArgument(fuseFsOpts.getFuseAuthPolicyCustomUser().isPresent()
            && !fuseFsOpts.getFuseAuthPolicyCustomUser().get().isEmpty(),
        String.format("%s should not be null or empty when using %s",
            PropertyKey.FUSE_AUTH_POLICY_CUSTOM_USER.getName(), className));
    Preconditions.checkArgument(fuseFsOpts.getFuseAuthPolicyCustomGroup().isPresent()
            && !fuseFsOpts.getFuseAuthPolicyCustomGroup().get().isEmpty(),
        String.format("%s should not be null or empty when using %s",
            PropertyKey.FUSE_AUTH_POLICY_CUSTOM_GROUP.getName(), className));
    String owner = fuseFsOpts.getFuseAuthPolicyCustomUser().get();
    String group = fuseFsOpts.getFuseAuthPolicyCustomGroup().get();
    mUid = AlluxioFuseUtils.getUid(owner);
    mGid = AlluxioFuseUtils.getGid(group);
    mSetAttributeOptions = SetAttributePOptions.newBuilder()
        .setOwner(owner).setGroup(group).build();
    LOG.debug("Created {} with owner [id {}, name {}] and group [id {}, name {}]",
        className, owner, mUid, group, mGid);
  }

  @Override
  public void setUserGroup(AlluxioURI uri) {
    try {
      mFileSystem.setAttribute(uri, mSetAttributeOptions);
    } catch (IOException | AlluxioException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long getUid(String owner) {
    return mUid;
  }

  @Override
  public long getGid(String group) {
    return mGid;
  }
}
