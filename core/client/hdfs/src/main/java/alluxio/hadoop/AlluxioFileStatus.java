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

package alluxio.hadoop;

import alluxio.client.file.URIStatus;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * A enhanced file status class that additional stores the uri status,
 * including the block location info so that we don't need to make an
 * additional RPC call to fetch the block location from the hadoop client.
 */
public class AlluxioFileStatus extends FileStatus {
  private static final long serialVersionUID = 750462858921667680L;
  private final URIStatus mUriStatus;

  /**
   * Constructs a new {@link AlluxioFileStatus} instance.
   *
   * @param status alluxio uri file status
   * @param fsPath filesystem path
   */
  public AlluxioFileStatus(URIStatus status, Path fsPath) {
    super(status.getLength(), status.isFolder(), status.getReplicationMin(),
        status.getBlockSizeBytes(), status.getLastModificationTimeMs(),
        status.getLastAccessTimeMs(), new FsPermission((short) status.getMode()),
        status.getOwner(), status.getGroup(), fsPath);
    mUriStatus = status;
  }

  /**
   * get uri status.
   *
   * @return uri status
   */
  public URIStatus getUriStatus() {
    return mUriStatus;
  }

  @Override
  public boolean equals(Object o) {
    // fix findbugs
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    // fix findbugs
    return super.hashCode();
  }
}
