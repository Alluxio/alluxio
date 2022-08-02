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

package alluxio.master.file.meta.crosscluster;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Objects;

/**
 * Keeps information about a mount and the addresses.
 */
public class MountSyncAddress {
  private final MountSync mMountSync;
  private final InetSocketAddress[] mAddresses;

  /**
   * Create a new mount sync with addresses.
   * @param mountSync the mount info
   * @param addresses the list of addresses
   */
  public MountSyncAddress(MountSync mountSync, InetSocketAddress[] addresses) {
    mMountSync = mountSync;
    mAddresses = addresses.clone();
  }

  /**
   * @return the mount sync
   */
  public MountSync getMountSync() {
    return mMountSync;
  }

  /**
   * @return the addresses
   */
  public InetSocketAddress[] getAddresses() {
    return mAddresses;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof MountSyncAddress) {
      MountSyncAddress other = (MountSyncAddress) o;
      return mMountSync.equals(other.mMountSync) && Arrays.equals(mAddresses, other.mAddresses);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(mMountSync, Arrays.hashCode(mAddresses));
  }
}
