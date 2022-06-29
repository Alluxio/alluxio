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
import alluxio.grpc.SetAttributePOptions;
import alluxio.jnifuse.AbstractFuseFileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * A Fuse authentication policy supports user-defined user and group.
 */
public class CustomAuthPolicy implements AuthPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(CustomAuthPolicy.class);
  private final FileSystem mFileSystem;
  private final Optional<String> mUname;
  private final Optional<String> mGname;

  /**
   * @param fileSystem     the Alluxio file system
   * @param fuseFsOpts     the options for AlluxioFuse filesystem
   * @param fuseFileSystem the FuseFileSystem
   */
  public CustomAuthPolicy(FileSystem fileSystem, AlluxioFuseFileSystemOpts fuseFsOpts,
      AbstractFuseFileSystem fuseFileSystem) {
    mFileSystem = fileSystem;
    mUname = fuseFsOpts.getFuseAuthPolicyCustomUser();
    mGname = fuseFsOpts.getFuseAuthPolicyCustomGroup();
  }

  @Override
  public void setUserGroupIfNeeded(AlluxioURI uri) {
    if (!mUname.isPresent() || !mGname.isPresent()) {
      return;
    }
    SetAttributePOptions attributeOptions = SetAttributePOptions.newBuilder()
        .setGroup(mGname.get())
        .setOwner(mUname.get())
        .build();
    try {
      mFileSystem.setAttribute(uri, attributeOptions);
    } catch (IOException | AlluxioException e) {
      throw new RuntimeException(e);
    }
    LOG.debug("Set attributes of path {} to {}", uri, attributeOptions);
  }
}
