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
import alluxio.grpc.MountPRequest;
import alluxio.grpc.UfsInfo;

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;

/**
 * Interface for the mount table including path conversion and CRUD logic.
 */
public interface MountTableManager extends Closeable {
  String ROOT = "/";

  /**
   * Converts an Alluxio path to UFS path according to the mount table.
   * If a matching UFS mount point is not found, {@code Optional.empty()} is returned.
   *
   * @param alluxioPath the Alluxio path to convert to UFS path
   * @return the corresponding UFS path for the file/dir
   */
  Optional<AlluxioURI> convertToUfsPath(AlluxioURI alluxioPath);

  /**
   * Converts a UFS path back to Alluxio path according to the mount table.
   * If a matching UFS mount point is not found, {@code Optional.empty()} is returned.
   *
   * @param ufsPath the UFS path to look for
   * @return the corresponding alluxio path
   */
  Optional<AlluxioURI> convertToAlluxioPath(AlluxioURI ufsPath);

  /**
   * Finds all info of the corresponding UFS for the given UFS path, including UFS configs.
   * If a matching UFS mount point is not found, {@code Optional.empty()} is returned.
   *
   * @param ufsPath the UFS path to look for
   * @return the full UFS info
   */
  Optional<UfsInfo> findUfsInfo(AlluxioURI ufsPath);

  /**
   * List all entries of the mount table in an unmodifiable view.
   * The key is the Alluxio path and the value is a {@link UfsInfo}.
   *
   * @return a mount table view
   */
  Map<String, UfsInfo> listMountTable();

  /**
   * Adds a mount point to the mount table.
   * If an implementation supports this operation, the change in mount table must be persisted
   * before the return of this method. In other words, immediately after this operation returns,
   * the change should be observable to another process.
   *
   * @param request a request that includes the alluxio-ufs path pair and configs
   */
  void addMountPoint(MountPRequest request);

  /**
   * Removes a mount point from the mount table.
   * If an implementation supports this operation, the change in mount table must be persisted
   * before the return of this method. In other words, immediately after this operation returns,
   * the change should be observable to another process.
   *
   * @param alluxioPath the key to the mount table entry
   */
  void removeMountPoint(AlluxioURI alluxioPath);

  /**
   * Checks if a path is a mount point in Alluxio namespace.
   *
   * @param alluxioPath the path to check assuming it is in Alluxio namespace
   * @return true only if the path is a mount point, false otherwise
   */
  boolean isMountPoint(AlluxioURI alluxioPath);

  /**
   * Type of MountTableManager to use.
   */
  enum Type {
    // The mount table is defined in etcd.
    ETCD,
    // The mount table is defined in a static configuration file.
    STATIC_FILE,
    // No mount table is used. For compatibility with Alluxio CE which does not support
    // mounting multiple UFSs.
    NONE
  }
}
