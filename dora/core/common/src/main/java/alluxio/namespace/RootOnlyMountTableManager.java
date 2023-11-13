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

package alluxio.namespace;

import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.MountPRequest;
import alluxio.grpc.UfsInfo;
import alluxio.util.io.PathUtils;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * A MountTableManager implementation that only has one root mount.
 * This is an emulator that matches Alluxio CE behavior.
 * it is simply an emulator that intends to match CE behavior with no regard to
 * the mount table manager definition and interfacing in the class structure.
 */
public class RootOnlyMountTableManager implements MountTableManager {
  String mUfsRootPath;
  AlluxioURI mUfsRootUri;
  // This instance is only used by listMountTable()
  UfsInfo mUfsInfo;

  /**
   * Constructs an instance of {@link RootOnlyMountTableManager}.
   * @param ufsPath
   */
  public RootOnlyMountTableManager(String ufsPath) {
    mUfsRootPath = ufsPath;
    mUfsRootUri = new AlluxioURI(ufsPath);
    mUfsInfo = UfsInfo.newBuilder()
        .setUri(ufsPath).setProperties(MountPOptions.getDefaultInstance()).build();
  }

  @Override
  public Optional<AlluxioURI> convertToUfsPath(AlluxioURI alluxioPath) {
    return Optional.of(PathUtils.convertAlluxioPathToUfsPath(alluxioPath, mUfsRootUri));
  }

  @Override
  public Optional<AlluxioURI> convertToAlluxioPath(AlluxioURI ufsPath) {
    try {
      return Optional.of(PathUtils.convertUfsPathToAlluxioPath(ufsPath, mUfsRootUri));
    } catch (InvalidPathException e) {
      return Optional.empty();
    }
  }

  @Override
  public Optional<UfsInfo> findUfsInfo(AlluxioURI ufsPath) {
    if (ufsPath.toString().startsWith(mUfsRootPath)) {
      return Optional.of(mUfsInfo);
    } else {
      return Optional.empty();
    }
  }

  @Override
  public Map<String, UfsInfo> listMountTable() {
    return ImmutableMap.of(mUfsRootPath, mUfsInfo);
  }

  @Override
  public void addMountPoint(MountPRequest request) {
    throw new UnsupportedOperationException("Cannot add mount points to "
        + RootOnlyMountTableManager.class.getSimpleName());
  }

  @Override
  public void removeMountPoint(AlluxioURI alluxioPath) {
    throw new UnsupportedOperationException("Cannot remove mount points from "
        + RootOnlyMountTableManager.class.getSimpleName());
  }

  @Override
  public boolean isMountPoint(AlluxioURI alluxioPath) {
    // Only root is a mount point
    return alluxioPath.isRoot();
  }

  @Override
  public void close() throws IOException {
    // noop
  }
}
