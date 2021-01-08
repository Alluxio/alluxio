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

package alluxio.util;

import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.TieredIdentity;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility functions for working with tiered identity.
 */
@ThreadSafe
public final class TieredIdentityUtils {

  /**
   * Locality comparison for wire type locality tiers, two locality tiers matches if both name and
   * values are equal, or for the "node" tier, if the node names resolve to the same IP address.
   *
   * @param tier a wire type locality tier
   * @param otherTier a wire type locality tier to compare to
   * @param resolveIpAddress whether or not to resolve hostnames to IP addresses for node locality
   * @return true if the wire type locality tier matches the given tier
   */
  public static boolean matches(TieredIdentity.LocalityTier tier,
      TieredIdentity.LocalityTier otherTier, boolean resolveIpAddress) {
    String otherTierName = otherTier.getTierName();
    if (!tier.getTierName().equals(otherTierName)) {
      return false;
    }
    String otherTierValue = otherTier.getValue();
    if (tier.getValue() != null && tier.getValue().equals(otherTierValue)) {
      return true;
    }
    // For node tiers, attempt to resolve hostnames to IP addresses, this avoids common
    // misconfiguration errors where a worker is using one hostname and the client is using
    // another.
    if (resolveIpAddress) {
      if (Constants.LOCALITY_NODE.equals(tier.getTierName())) {
        try {
          String tierIpAddress = NetworkAddressUtils.resolveIpAddress(tier.getValue());
          String otherTierIpAddress = NetworkAddressUtils.resolveIpAddress(otherTierValue);
          if (tierIpAddress != null && tierIpAddress.equals(otherTierIpAddress)) {
            return true;
          }
        } catch (UnknownHostException e) {
          return false;
        }
      }
    }
    return false;
  }

  /**
   * @param tieredIdentity the tiered identity
   * @param identities the tiered identities to compare to
   * @param conf Alluxio configuration
   * @return the identity closest to this one. If none of the identities match, the first identity
   *         is returned
   */
  public static Optional<TieredIdentity> nearest(TieredIdentity tieredIdentity,
      List<TieredIdentity> identities, AlluxioConfiguration conf) {
    if (identities.isEmpty()) {
      return Optional.empty();
    }
    for (TieredIdentity.LocalityTier tier : tieredIdentity.getTiers()) {
      if (tier == null) {
        return Optional.of(identities.get(0));
      }
      for (TieredIdentity identity : identities) {
        for (TieredIdentity.LocalityTier otherTier : identity.getTiers()) {
          if (matches(tier, otherTier, conf.getBoolean(PropertyKey.LOCALITY_COMPARE_NODE_IP))) {
            return Optional.of(identity);
          }
        }
      }
    }
    return Optional.of(identities.get(0));
  }

  private TieredIdentityUtils() {} // prevent instantiation
}
