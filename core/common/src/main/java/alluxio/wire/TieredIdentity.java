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

package alluxio.wire;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.util.network.NetworkAddressUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * Class representing a node's tier identity. A tier identity is a list of locality tiers
 * identifying network topology, e.g. (host: hostname, rack: rack1).
 */
@PublicApi
public final class TieredIdentity implements Serializable {
  private static final long serialVersionUID = -1920596090085594788L;

  private final List<LocalityTier> mTiers;

  /**
   * @param tiers the tiers of the tier identity
   */
  @JsonCreator
  public TieredIdentity(@JsonProperty("tiers") List<LocalityTier> tiers) {
    mTiers = ImmutableList.copyOf(Preconditions.checkNotNull(tiers, "tiers"));
  }

  /**
   * @return the tiers of the tier identity
   */
  public List<LocalityTier> getTiers() {
    return mTiers;
  }

  /**
   * @param i a tier index
   * @return the ith locality tier
   */
  public LocalityTier getTier(int i) {
    return mTiers.get(i);
  }

  /**
   * @return a Thrift representation
   */
  public alluxio.thrift.TieredIdentity toThrift() {
    return new alluxio.thrift.TieredIdentity(mTiers.stream()
        .map(LocalityTier::toThrift).collect(Collectors.toList())
    );
  }

  /**
   * @param tieredIdentity a Thrift tiered identity
   * @return the corresponding wire type tiered identity
   */
  @Nullable
  public static TieredIdentity fromThrift(alluxio.thrift.TieredIdentity tieredIdentity) {
    if (tieredIdentity == null) {
      return null;
    }
    return new TieredIdentity(tieredIdentity.getTiers().stream()
        .map(LocalityTier::fromThrift).collect(Collectors.toList()));
  }

  /**
   * @param identities the tiered identities to compare to
   * @return the identity closest to this one. If none of the identities match, the first identity
   *         is returned
   */
  public Optional<TieredIdentity> nearest(List<TieredIdentity> identities) {
    if (identities.isEmpty()) {
      return Optional.empty();
    }
    for (LocalityTier tier : mTiers) {
      for (TieredIdentity identity : identities) {
        for (LocalityTier otherTier : identity.mTiers) {
          if (tier != null && tier.matches(otherTier)) {
            return Optional.of(identity);
          }
        }
      }
    }
    return Optional.of(identities.get(0));
  }

  /**
   * @param other a tiered identity to compare to
   * @return whether the top tier of this tiered identity matches the top tier of other
   */
  public boolean topTiersMatch(TieredIdentity other) {
    return mTiers.get(0).equals(other.getTier(0));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TieredIdentity)) {
      return false;
    }
    TieredIdentity that = (TieredIdentity) o;
    return mTiers.equals(that.mTiers);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mTiers);
  }

  @Override
  public String toString() {
    String tiers = Joiner.on(", ").join(mTiers.stream()
        .map(tier -> tier.getTierName() + "=" + tier.getValue())
        .collect(Collectors.toList()));
    return String.format("TieredIdentity(%s)", tiers);
  }

  /**
   * Class representing a locality tier, e.g. (host: hostname).
   */
  public static final class LocalityTier implements Serializable {
    private static final long serialVersionUID = 7078638137905293841L;

    private final String mTierName;
    private final String mValue;

    /**
     * @param tierName the name of the tier
     * @param value the value of the tier
     */
    @JsonCreator
    public LocalityTier(@JsonProperty("tierName") String tierName,
        @JsonProperty("value") @Nullable String value) {
      mTierName = Preconditions.checkNotNull(tierName, "tierName");
      mValue = value;
    }

    /**
     * @return the name of the tier
     */
    public String getTierName() {
      return mTierName;
    }

    /**
     * @return the value
     */
    @Nullable
    public String getValue() {
      return mValue;
    }

    /**
     * @return a Thrift representation
     */
    public alluxio.thrift.LocalityTier toThrift() {
      return new alluxio.thrift.LocalityTier(mTierName, mValue);
    }

    /**
     * @param localityTier a Thrift locality tier
     * @return the corresponding wire type locality tier
     */
    public static LocalityTier fromThrift(alluxio.thrift.LocalityTier localityTier) {
      return new LocalityTier(localityTier.getTierName(), localityTier.getValue());
    }

    /**
     * Locality comparison for wire type locality tiers, two locality tiers matches if both name
     * and values are equal, or for the "node" tier, if the node names resolve to the same
     * IP address.
     *
     * @param otherTier a wire type locality tier to compare to
     * @return true if the wire type locality tier matches the given tier
     */
    public boolean matches(LocalityTier otherTier) {
      String otherTierName = otherTier.getTierName();
      if (!mTierName.equals(otherTierName)) {
        return false;
      }
      String otherTierValue = otherTier.getValue();
      if (mValue != null && mValue.equals(otherTierValue)) {
        return true;
      }
      // For node tiers, attempt to resolve hostnames to IP addresses, this avoids common
      // misconfiguration errors where a worker is using one hostname and the client is using
      // another.
      if (Configuration.getBoolean(PropertyKey.LOCALITY_COMPARE_NODE_IP)) {
        if (Constants.LOCALITY_NODE.equals(mTierName)) {
          try {
            String tierIpAddress = NetworkAddressUtils.resolveIpAddress(mValue);
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof LocalityTier)) {
        return false;
      }
      LocalityTier that = (LocalityTier) o;
      return mTierName.equals(that.mTierName) && Objects.equal(mValue, that.mValue);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mTierName, mValue);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("tierName", mTierName)
          .add("value", mValue)
          .toString();
    }
  }
}
