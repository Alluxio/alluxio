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
import alluxio.jnifuse.FuseFileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom Fuse Auth Policy.
 */
public class CustomAuthPolicy implements AuthPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(CustomAuthPolicy.class);
  private final FileSystem mFileSystem;
  private final String mUname;
  private final String mGname;

  /**
   * @param fileSystem     - FileSystem
   * @param conf           - AlluxioConfiguration
   * @param fuseFileSystem - FuseFileSystem
   */
  public CustomAuthPolicy(FileSystem fileSystem, AlluxioConfiguration conf,
      FuseFileSystem fuseFileSystem) {
    mFileSystem = fileSystem;
    mUname = conf.get(PropertyKey.FUSE_AUTH_POLICY_CUSTOM_USER);
    mGname = conf.get(PropertyKey.FUSE_AUTH_POLICY_CUSTOM_GROUP);
  }

  @Override
  public void setUserGroupIfNeeded(AlluxioURI uri) throws Exception {
    if (mUname == null || mGname == null) {
      return;
    }
    SetAttributePOptions attributeOptions = SetAttributePOptions.newBuilder()
        .setGroup(mUname)
        .setOwner(mGname)
        .build();
    LOG.debug("Set attributes of path {} to {}", uri, attributeOptions);
    mFileSystem.setAttribute(uri, attributeOptions);
  }
}
