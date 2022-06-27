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
import java.util.concurrent.ExecutionException;

/**
 * A Fuse authentication policy that follows the default security implementation.
 * By default, the user group is set to the user that launches the Fuse application.
 */
public class FuseUserGroupAuthPolicy implements AuthPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(
      FuseUserGroupAuthPolicy.class);

  protected final FileSystem mFileSystem;
  protected final AbstractFuseFileSystem mFuseFileSystem;
  protected final AlluxioFuseFileSystemOpts mFuseOptions;

  private final LoadingCache<Long, String> mUsernameCache = CacheBuilder.newBuilder()
      .maximumSize(100)
      .build(new CacheLoader<Long, String>() {
        @Override
        public String load(Long uid) {
          return AlluxioFuseUtils.getUserName(uid);
        }
      });
  private final LoadingCache<Long, String> mGroupnameCache = CacheBuilder.newBuilder()
      .maximumSize(100)
      .build(new CacheLoader<Long, String>() {
        @Override
        public String load(Long gid) {
          return AlluxioFuseUtils.getGroupName(gid);
        }
      });

  /**
   * @param fileSystem     the Alluxio file system
   * @param fuseFsOpts     the options for AlluxioFuse filesystem
   * @param fuseFileSystem the FuseFileSystem
   */
  public FuseUserGroupAuthPolicy(FileSystem fileSystem, AlluxioFuseFileSystemOpts fuseFsOpts,
      AbstractFuseFileSystem fuseFileSystem) {
    mFileSystem = fileSystem;
    mFuseFileSystem = fuseFileSystem;
    mFuseOptions = fuseFsOpts;
  }

  @Override
  public void setUserGroup(AlluxioURI uri) {
    // By default, set the user group as the user/group that launches the Fuse application
  }

  @Override
  public void setUserGroup(AlluxioURI uri, long uid, long gid) {
    if (uid == AlluxioFuseUtils.ID_NOT_SET_VALUE
        || uid == AlluxioFuseUtils.ID_NOT_SET_VALUE_UNSIGNED
        || gid == AlluxioFuseUtils.ID_NOT_SET_VALUE
        || gid == AlluxioFuseUtils.ID_NOT_SET_VALUE_UNSIGNED) {
      // cannot get valid uid or gid
      return;
    }
    if (uid == AlluxioFuseUtils.DEFAULT_UID && gid == AlluxioFuseUtils.DEFAULT_GID) {
      // no need to set attribute
      return;
    }
    try {
      String userName = uid != AlluxioFuseUtils.DEFAULT_UID
          ? mUsernameCache.get(uid) : AlluxioFuseUtils.DEFAULT_USER_NAME;
      String groupName = gid != AlluxioFuseUtils.DEFAULT_GID
          ? mGroupnameCache.get(gid) : AlluxioFuseUtils.DEFAULT_GROUP_NAME;
      if (userName.isEmpty() || groupName.isEmpty()) {
        // cannot get valid user name and group name
        return;
      }
      SetAttributePOptions attributeOptions = SetAttributePOptions.newBuilder()
          .setGroup(groupName)
          .setOwner(userName)
          .build();
      LOG.debug("Set attributes of path {} to {}", uri, attributeOptions);
      mFileSystem.setAttribute(uri, attributeOptions);
    } catch (IOException | ExecutionException | AlluxioException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long getUid(String owner) {
    return AlluxioFuseUtils.DEFAULT_UID;
  }

  @Override
  public long getGid(String group) {
    return AlluxioFuseUtils.DEFAULT_GID;
  }
}
