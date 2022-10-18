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
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.grpc.SetAttributePOptions;
import alluxio.jnifuse.FuseFileSystem;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * A Fuse authentication policy supports user-defined user and group.
 */
public class CustomAuthPolicy extends LaunchUserGroupAuthPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(CustomAuthPolicy.class);
  private final long mUid;
  private final long mGid;
  private final SetAttributePOptions mSetAttributeOptions;

  /**
   * Creates a new custom auth policy.
   *
   * @param fileSystem file system
   * @param conf the Alluxio configuration
   * @param fuseFileSystem fuse file system
   * @return custom auth policy
   */
  public static CustomAuthPolicy create(FileSystem fileSystem,
      AlluxioConfiguration conf, Optional<FuseFileSystem> fuseFileSystem) {
    String className = CustomAuthPolicy.class.getName();
    String owner = conf.getString(PropertyKey.FUSE_AUTH_POLICY_CUSTOM_USER);
    String group = conf.getString(PropertyKey.FUSE_AUTH_POLICY_CUSTOM_GROUP);
    Preconditions.checkArgument(!owner.isEmpty(),
        String.format("%s should not be null or empty when using %s",
            PropertyKey.FUSE_AUTH_POLICY_CUSTOM_USER.getName(), className));
    Preconditions.checkArgument(!group.isEmpty(),
        String.format("%s should not be null or empty when using %s",
            PropertyKey.FUSE_AUTH_POLICY_CUSTOM_GROUP.getName(), className));
    Optional<Long> uid = AlluxioFuseUtils.getUid(owner);
    Optional<Long> gid = AlluxioFuseUtils.getGidFromGroupName(group);
    if (!uid.isPresent()) {
      throw new RuntimeException(String
          .format("Cannot create %s with invalid owner %s: failed to get uid", className, owner));
    }
    if (!gid.isPresent()) {
      throw new RuntimeException(String
          .format("Cannot create %s with invalid group %s: failed to get gid", className, group));
    }
    SetAttributePOptions setAttributeOptions = SetAttributePOptions.newBuilder()
        .setOwner(owner).setGroup(group).build();
    LOG.info("Creating {} with owner [id {}, name {}] and group [id {}, name {}]",
        className, owner, uid, group, gid);
    return new CustomAuthPolicy(fileSystem, fuseFileSystem,
        uid.get(), gid.get(), setAttributeOptions);
  }

  /**
   * @param fileSystem     the Alluxio file system
   * @param fuseFileSystem the FuseFileSystem
   */
  private CustomAuthPolicy(FileSystem fileSystem, Optional<FuseFileSystem> fuseFileSystem,
      long uid, long gid, SetAttributePOptions options) {
    super(fileSystem, fuseFileSystem);
    mUid = uid;
    mGid = gid;
    mSetAttributeOptions = options;
  }

  @Override
  public void setUserGroupIfNeeded(AlluxioURI uri) {
    try {
      mFileSystem.setAttribute(uri, mSetAttributeOptions);
    } catch (IOException | AlluxioException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<Long> getUid(String owner) {
    return Optional.of(mUid);
  }

  @Override
  public Optional<Long> getGid(String group) {
    return Optional.of(mGid);
  }
}
