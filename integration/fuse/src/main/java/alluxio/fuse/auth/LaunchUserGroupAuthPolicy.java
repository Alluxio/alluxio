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
import alluxio.exception.AlluxioException;
import alluxio.fuse.AlluxioFuseFileSystemOpts;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.grpc.SetAttributePOptions;
import alluxio.jnifuse.AbstractFuseFileSystem;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * An authentication policy that follows the default security implementation
 * that the user group is set to the user that launches the Fuse application.
 */
public class LaunchUserGroupAuthPolicy implements AuthPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(
      LaunchUserGroupAuthPolicy.class);

  protected final FileSystem mFileSystem;
  protected final Optional<AbstractFuseFileSystem> mFuseFileSystem;
  protected final AlluxioFuseFileSystemOpts mFuseOptions;

  private final LoadingCache<Long, Optional<String>> mUsernameCache = CacheBuilder.newBuilder()
      .maximumSize(100)
      .build(new CacheLoader<Long, Optional<String>>() {
        @Override
        public Optional<String> load(Long uid) {
          return AlluxioFuseUtils.getUserName(uid);
        }
      });
  private final LoadingCache<Long, Optional<String>> mGroupnameCache = CacheBuilder.newBuilder()
      .maximumSize(100)
      .build(new CacheLoader<Long, Optional<String>>() {
        @Override
        public Optional<String> load(Long gid) {
          return AlluxioFuseUtils.getGroupName(gid);
        }
      });

  protected long mLaunchUserId;
  protected String mLaunchUserName;
  protected long mLaunchGroupId;
  protected String mLaunchGroupName;

  /**
   * Creates a new launch user auth policy.
   *
   * @param fileSystem file system
   * @param fuseFsOpts fuse options
   * @param fuseFileSystem fuse file system
   * @return launch user auth policy
   */
  public static LaunchUserGroupAuthPolicy create(FileSystem fileSystem,
      AlluxioFuseFileSystemOpts fuseFsOpts, Optional<AbstractFuseFileSystem> fuseFileSystem) {
    return new LaunchUserGroupAuthPolicy(fileSystem, fuseFsOpts, fuseFileSystem);
  }

  /**
   * @param fileSystem     the Alluxio file system
   * @param fuseFsOpts     the options for AlluxioFuse filesystem
   * @param fuseFileSystem the FuseFileSystem
   */
  protected LaunchUserGroupAuthPolicy(FileSystem fileSystem, AlluxioFuseFileSystemOpts fuseFsOpts,
      Optional<AbstractFuseFileSystem> fuseFileSystem) {
    mFileSystem = Preconditions.checkNotNull(fileSystem);
    mFuseOptions = Preconditions.checkNotNull(fuseFsOpts);
    mFuseFileSystem = fuseFileSystem;
  }

  @Override
  public void init() {
    String launchUser = System.getProperty("user.name");
    if (launchUser == null || launchUser.isEmpty()) {
      throw new RuntimeException("Failed to init authentication policy: failed to get launch user");
    }
    mLaunchUserName = launchUser;
    Optional<Long> launchUserId = AlluxioFuseUtils.getUid(mLaunchUserName);
    if (!launchUserId.isPresent()) {
      throw new RuntimeException(
          "Failed to init authentication policy: failed to get uid of launch user "
              + mLaunchUserName);
    }
    mLaunchUserId = launchUserId.get();
    Optional<String> launchGroupName = AlluxioFuseUtils.getGroupName(mLaunchUserName);
    if (!launchGroupName.isPresent()) {
      throw new RuntimeException(
          "Failed to init authentication policy: failed to get group name from user name "
              + mLaunchUserName);
    }
    mLaunchGroupName = launchGroupName.get();
    Optional<Long> launchGroupId = AlluxioFuseUtils.getGid(mLaunchGroupName);
    if (!launchGroupId.isPresent()) {
      throw new RuntimeException(
          "Failed to init authentication policy: failed to get gid of launch group "
              + mLaunchGroupName);
    }
    mLaunchGroupId = launchGroupId.get();
    LOG.info(
        "Initialized Fuse auth policy with launch user (id:{}, name:{}) and group (id:{}, name:{})",
        mLaunchUserId, mLaunchUserName, mLaunchGroupId, mLaunchGroupName);
  }

  @Override
  public void setUserGroupIfNeeded(AlluxioURI uri) {
    // By default, Alluxio Fuse/client sets user/group to the user launches the Fuse application
    // no extra user group setting required
  }

  @Override
  public void setUserGroup(AlluxioURI uri, long uid, long gid) {
    if (uid == mLaunchUserId && gid == mLaunchGroupId) {
      // no need to set attribute
      return;
    }
    try {
      SetAttributePOptions.Builder attributeBuilder = SetAttributePOptions.newBuilder();
      mUsernameCache.get(uid).ifPresent(attributeBuilder::setOwner);
      mGroupnameCache.get(gid).ifPresent(attributeBuilder::setGroup);
      SetAttributePOptions attributeOptions = attributeBuilder.build();
      LOG.debug("Setting attributes of path {} to {}", uri, attributeOptions);
      mFileSystem.setAttribute(uri, attributeOptions);
    } catch (IOException | ExecutionException | AlluxioException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long getUid(String owner) {
    return mLaunchUserId;
  }

  @Override
  public long getGid(String group) {
    return mLaunchGroupId;
  }
}
