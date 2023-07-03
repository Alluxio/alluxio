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
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.jnifuse.FuseFileSystem;
import alluxio.jnifuse.struct.FuseContext;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * The system user group authentication policy that always set the user group
 * to the actual end user.
 * Note that this may downgrade the performance of creating file/directory.
 */
public final class SystemUserGroupAuthPolicy extends LaunchUserGroupAuthPolicy {

  private final LoadingCache<String, Optional<Long>> mUidCache = CacheBuilder.newBuilder()
      .maximumSize(Configuration.getInt(PropertyKey.FUSE_AUTH_POLICY_SYSTEM_CACHE_USER_GROUP_SIZE))
      .expireAfterWrite(Configuration.getDuration(
          PropertyKey.FUSE_AUTH_POLICY_SYSTEM_CACHE_USER_GROUP_EXPIRE_TIME))
      .build(new CacheLoader<String, Optional<Long>>() {
        @Override
        public Optional<Long> load(String username) {
          return AlluxioFuseUtils.getUid(username);
        }
      });
  private final LoadingCache<String, Optional<Long>> mGidCache = CacheBuilder.newBuilder()
      .maximumSize(Configuration.getInt(PropertyKey.FUSE_AUTH_POLICY_SYSTEM_CACHE_USER_GROUP_SIZE))
      .expireAfterWrite(Configuration.getDuration(
          PropertyKey.FUSE_AUTH_POLICY_SYSTEM_CACHE_USER_GROUP_EXPIRE_TIME))
      .build(new CacheLoader<String, Optional<Long>>() {
        @Override
        public Optional<Long> load(String group) {
          return AlluxioFuseUtils.getGidFromGroupName(group);
        }
      });

  /**
   * Creates a new system auth policy.
   *
   * @param fileSystem file system
   * @param conf the Alluxio configuration
   * @param fuseFileSystem fuse file system
   * @return system auth policy
   */
  public static SystemUserGroupAuthPolicy create(FileSystem fileSystem,
      AlluxioConfiguration conf, Optional<FuseFileSystem> fuseFileSystem) {
    return new SystemUserGroupAuthPolicy(fileSystem, fuseFileSystem);
  }

  /**
   * @param fileSystem     the Alluxio file system
   * @param fuseFileSystem AbstractFuseFileSystem
   */
  private SystemUserGroupAuthPolicy(FileSystem fileSystem,
      Optional<FuseFileSystem> fuseFileSystem) {
    super(fileSystem, fuseFileSystem);
    Preconditions.checkArgument(mFuseFileSystem.isPresent());
  }

  @Override
  public void setUserGroupIfNeeded(AlluxioURI uri) {
    FuseContext fc = mFuseFileSystem.get().getContext();
    setUserGroup(uri, fc.uid.get(), fc.gid.get());
  }

  @Override
  public Optional<Long> getUid() {
    return Optional.of(mFuseFileSystem.get().getContext().uid.get());
  }

  @Override
  public Optional<Long> getUid(String owner) {
    try {
      return mUidCache.get(owner);
    } catch (ExecutionException e) {
      return Optional.empty();
    }
  }

  @Override
  public Optional<Long> getGid() {
    return Optional.of(mFuseFileSystem.get().getContext().gid.get());
  }

  @Override
  public Optional<Long> getGid(String group) {
    try {
      return mGidCache.get(group);
    } catch (ExecutionException e) {
      return Optional.empty();
    }
  }
}
