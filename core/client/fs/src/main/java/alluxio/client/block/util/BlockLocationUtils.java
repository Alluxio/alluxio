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

package alluxio.client.block.util;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

import alluxio.Constants;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.TieredIdentityUtils;
import alluxio.util.network.NettyUtils;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;

/**
 * Utility functions for working with block locations.
 */
@ThreadSafe
public final class BlockLocationUtils {

  /**
   * @param tieredIdentity the tiered identity
   * @param addresses the candidate worker addresses
   * @param conf Alluxio configuration
   * @return the address closest to this one. If none of the identities match, the first address is
   *         returned
   */
  public static Optional<Pair<WorkerNetAddress, Boolean>> nearest(TieredIdentity tieredIdentity,
      List<WorkerNetAddress> addresses, AlluxioConfiguration conf) {
    if (conf.getBoolean(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID)) {
      // Determine by inspecting the file system if worker is local
      for (WorkerNetAddress addr : addresses) {
        if (NettyUtils.isDomainSocketSupported(addr, conf)) {
          // Returns the first local worker and does not shuffle
          return Optional.of(new Pair<>(addr, true));
        }
      }
    }
    // Find nearest tiered identity
    Optional<TieredIdentity> nearestIdentity = TieredIdentityUtils.nearest(tieredIdentity,
        addresses.stream().map(addr -> addr.getTieredIdentity()).collect(Collectors.toList()),
        conf);
    if (!nearestIdentity.isPresent()) {
      return Optional.empty();
    }
    boolean isLocal = tieredIdentity.getTier(0).getTierName().equals(Constants.LOCALITY_NODE)
        && tieredIdentity.topTiersMatch(nearestIdentity.get());
    Optional<WorkerNetAddress> dataSource = addresses.stream()
        .filter(addr -> addr.getTieredIdentity().equals(nearestIdentity.get())).findFirst();
    if (!dataSource.isPresent()) {
      return Optional.empty();
    }
    return Optional.of(new Pair<>(dataSource.get(), isLocal));
  }

  private BlockLocationUtils() {} // prevent instantiation
}
