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
import alluxio.grpc.SetAttributePOptions;
import alluxio.jnifuse.AbstractFuseFileSystem;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Fuse authentication policy supports user-defined user and group.
 */
public class CustomAuthPolicy implements AuthPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(CustomAuthPolicy.class);
  private final FileSystem mFileSystem;
  private final String mUname;
  private final String mGname;

  /**
   * @param fileSystem     the Alluxio file system
   * @param conf           alluxio configuration
   * @param fuseFileSystem the FuseFileSystem
   */
  public CustomAuthPolicy(FileSystem fileSystem, AlluxioConfiguration conf,
      AbstractFuseFileSystem fuseFileSystem) {
    mFileSystem = fileSystem;
    mUname = conf.get(PropertyKey.FUSE_AUTH_POLICY_CUSTOM_USER);
    mGname = conf.get(PropertyKey.FUSE_AUTH_POLICY_CUSTOM_GROUP);
  }

  @Override
  public void setUserGroupIfNeeded(AlluxioURI uri) throws Exception {
    if (StringUtils.isEmpty(mUname) || StringUtils.isEmpty(mGname)) {
      return;
    }
    SetAttributePOptions attributeOptions = SetAttributePOptions.newBuilder()
        .setGroup(mUname)
        .setOwner(mGname)
        .build();
    mFileSystem.setAttribute(uri, attributeOptions);
    LOG.debug("Set attributes of path {} to {}", uri, attributeOptions);
  }
}
