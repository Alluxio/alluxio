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
  protected final AbstractFuseFileSystem mFuseFileSystem;
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
  protected Optional<String> mLaunchUserName;
  protected long mLaunchGroupId;
  protected Optional<String> mLaunchGroupName;

  /**
   * @param fileSystem     the Alluxio file system
   * @param fuseFsOpts     the options for AlluxioFuse filesystem
   * @param fuseFileSystem the FuseFileSystem
   */
  public LaunchUserGroupAuthPolicy(FileSystem fileSystem, AlluxioFuseFileSystemOpts fuseFsOpts,
      AbstractFuseFileSystem fuseFileSystem) {
    mFileSystem = fileSystem;
    mFuseFileSystem = fuseFileSystem;
    mFuseOptions = fuseFsOpts;
  }

  @Override
  public void init() {
    String launchUser = System.getProperty("user.name");
    mLaunchUserName = launchUser == null || launchUser.isEmpty()
        ? Optional.empty() : Optional.of(launchUser);
    mLaunchUserId = mLaunchUserName.map(AlluxioFuseUtils::getUid)
        .orElse(AlluxioFuseUtils.ID_NOT_SET_VALUE);
    mLaunchGroupName = mLaunchUserName.flatMap(AlluxioFuseUtils::getGroupName);
    mLaunchGroupId = mLaunchGroupName.map(AlluxioFuseUtils::getGidFromGroupName)
        .orElse(AlluxioFuseUtils.ID_NOT_SET_VALUE);
    String info = String.format("user (id:%s, name:%s) and group (id:%s, name:%s)",
        mLaunchUserId, mLaunchUserName.orElse("N/A"),
        mLaunchGroupId, mLaunchGroupName.orElse("N/A"));
    if (!mLaunchUserName.isPresent() | mLaunchUserId == AlluxioFuseUtils.ID_NOT_SET_VALUE
        || !mLaunchGroupName.isPresent() | mLaunchGroupId == AlluxioFuseUtils.ID_NOT_SET_VALUE) {
      LOG.warn("Failed to get valid Fuse launch user group: " + info);
    }
    LOG.info("Initialized Fuse auth policy with default " + info);
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
