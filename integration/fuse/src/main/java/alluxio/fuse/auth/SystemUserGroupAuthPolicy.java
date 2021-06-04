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
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.grpc.SetAttributePOptions;
import alluxio.jnifuse.AbstractFuseFileSystem;
import alluxio.jnifuse.struct.FuseContext;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Default Fuse Auth Policy.
 */
public final class SystemUserGroupAuthPolicy implements AuthPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(
      SystemUserGroupAuthPolicy.class);
  private static final String DEFAULT_USER_NAME = System.getProperty("user.name");
  private static final String DEFAULT_GROUP_NAME = System.getProperty("user.name");
  public static final long DEFAULT_UID = AlluxioFuseUtils.getUid(DEFAULT_USER_NAME);
  public static final long DEFAULT_GID = AlluxioFuseUtils.getGid(DEFAULT_GROUP_NAME);

  private final FileSystem mFileSystem;
  private final AbstractFuseFileSystem mFuseFileSystem;
  private final boolean mIsUserGroupTranslation;
  private final LoadingCache<Long, String> mUsernameCache;
  private final LoadingCache<Long, String> mGroupnameCache;

  /**
   * @param fileSystem     the Alluxio file system
   * @param conf           alluxio configuration
   * @param fuseFileSystem AbstractFuseFileSystem
   */
  public SystemUserGroupAuthPolicy(
      FileSystem fileSystem, AlluxioConfiguration conf, AbstractFuseFileSystem fuseFileSystem) {
    mFileSystem = fileSystem;
    mFuseFileSystem = fuseFileSystem;

    mUsernameCache = CacheBuilder.newBuilder()
        .maximumSize(100)
        .build(new CacheLoader<Long, String>() {
          @Override
          public String load(Long uid) {
            try {
              String userName = AlluxioFuseUtils.getGroupName(uid);
              return userName.isEmpty() ? DEFAULT_USER_NAME : userName;
            } catch (IOException e) {
              // This should never be reached since input uid is always valid
              LOG.error("Failed to get user name from uid {}, fallback to {}",
                  uid, DEFAULT_USER_NAME);
              return DEFAULT_USER_NAME;
            }
          }
        });
    mGroupnameCache = CacheBuilder.newBuilder()
        .maximumSize(100)
        .build(new CacheLoader<Long, String>() {
          @Override
          public String load(Long gid) {
            try {
              String groupName = AlluxioFuseUtils.getGroupName(gid);
              return groupName.isEmpty() ? DEFAULT_GROUP_NAME : groupName;
            } catch (IOException e) {
              // This should never be reached since input gid is always valid
              LOG.error("Failed to get group name from gid {}, fallback to {}.",
                  gid, DEFAULT_GROUP_NAME);
              return DEFAULT_GROUP_NAME;
            }
          }
        });
    mIsUserGroupTranslation = conf.getBoolean(PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED);
  }

  @Override
  public void setUserGroupIfNeeded(AlluxioURI uri) throws Exception {
    FuseContext fc = mFuseFileSystem.getContext();
    long uid = mIsUserGroupTranslation ? fc.uid.get() : DEFAULT_UID;
    long gid = mIsUserGroupTranslation ? fc.gid.get() : DEFAULT_GID;
    if (gid != DEFAULT_GID || uid != DEFAULT_UID) {
      String groupName = gid != DEFAULT_GID ? mGroupnameCache.get(gid) : DEFAULT_GROUP_NAME;
      String userName = uid != DEFAULT_UID ? mUsernameCache.get(uid) : DEFAULT_USER_NAME;
      SetAttributePOptions attributeOptions = SetAttributePOptions.newBuilder()
          .setGroup(groupName)
          .setOwner(userName)
          .build();
      LOG.debug("Set attributes of path {} to {}", uri, attributeOptions);
      mFileSystem.setAttribute(uri, attributeOptions);
    }
  }
}
