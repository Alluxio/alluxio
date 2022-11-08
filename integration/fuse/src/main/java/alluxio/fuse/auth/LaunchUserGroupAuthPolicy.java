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
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.grpc.SetAttributePOptions;
import alluxio.jnifuse.FuseFileSystem;

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
  protected final Optional<FuseFileSystem> mFuseFileSystem;

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

  private long mLaunchUserId;
  private long mLaunchGroupId;

  /**
   * Creates a new launch user auth policy.
   *
   * @param fileSystem file system
   * @param conf the Alluxio configuration
   * @param fuseFileSystem fuse file system
   * @return launch user auth policy
   */
  public static LaunchUserGroupAuthPolicy create(FileSystem fileSystem,
      AlluxioConfiguration conf, Optional<FuseFileSystem> fuseFileSystem) {
    return new LaunchUserGroupAuthPolicy(fileSystem, fuseFileSystem);
  }

  /**
   * @param fileSystem     the Alluxio file system
   * @param fuseFileSystem the FuseFileSystem
   */
  protected LaunchUserGroupAuthPolicy(FileSystem fileSystem,
      Optional<FuseFileSystem> fuseFileSystem) {
    mFileSystem = Preconditions.checkNotNull(fileSystem);
    mFuseFileSystem = Preconditions.checkNotNull(fuseFileSystem);
  }

  @Override
  public void init() {
    mLaunchUserId = AlluxioFuseUtils.getSystemUid();
    mLaunchGroupId = AlluxioFuseUtils.getSystemGid();
    LOG.info(
        "Initialized Fuse auth policy with launch user (id:{}) and group (id:{})",
        mLaunchUserId, mLaunchGroupId);
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

    Optional<URIStatus> status = AlluxioFuseUtils.getPathStatus(mFileSystem, uri);
    try {
      // Avoid setUserGroup if the file already has correct owner and group
      if (status.isPresent()
          && mUsernameCache.get(uid).isPresent()
          && mGroupnameCache.get(gid).isPresent()
          && status.get().getOwner().equals(mUsernameCache.get(uid).get())
          && status.get().getGroup().equals(mGroupnameCache.get(gid).get())) {
        return;
      }

      SetAttributePOptions.Builder attributeBuilder = SetAttributePOptions.newBuilder();
      mUsernameCache.get(uid).ifPresent(attributeBuilder::setOwner);
      mGroupnameCache.get(gid).ifPresent(attributeBuilder::setGroup);
      SetAttributePOptions attributeOptions = attributeBuilder.build();
      LOG.debug("Setting attributes of path {} to {}", uri, attributeOptions);
      mFileSystem.setAttribute(uri, attributeOptions);
    } catch (IOException | ExecutionException | AlluxioException e) {
      throw AlluxioRuntimeException.from(e);
    }
  }

  @Override
  public Optional<Long> getUid() {
    return Optional.of(mLaunchUserId);
  }

  @Override
  public Optional<Long> getGid() {
    return Optional.of(mLaunchGroupId);
  }
}
