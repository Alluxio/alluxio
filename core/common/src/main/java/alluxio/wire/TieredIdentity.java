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
import alluxio.PropertyKey;
import alluxio.PropertyKey.Template;
import alluxio.annotation.PublicApi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * Class representing a node's tier identity. A tier identity is a list of locality tiers
 * identifying network topology, e.g. (host: hostname, rack: rack1).
 */
@PublicApi
public final class TieredIdentity {
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
  public static TieredIdentity fromThrift(alluxio.thrift.TieredIdentity tieredIdentity) {
    if (tieredIdentity == null) {
      return null;
    }
    return new TieredIdentity(tieredIdentity.getTiers().stream()
        .map(LocalityTier::fromThrift).collect(Collectors.toList()));
  }

  /**
   * @param identities the tiered identities to compare to
   * @return the identity closest to this one; or Optional.empty if none of the identities match
   *         within a strict tier. If none of the identities match and no strict tiers are defined,
   *         the first identity is returned
   */
  public Optional<TieredIdentity> nearest(List<TieredIdentity> identities) {
    if (identities.isEmpty()) {
      return Optional.empty();
    }
    for (LocalityTier tier : mTiers) {
      for (TieredIdentity identity : identities) {
        for (LocalityTier otherTier : identity.mTiers) {
          if (tier.mTierName.equals(otherTier.mTierName)
              && tier.mValue != null
              && tier.mValue.equals(otherTier.mValue)) {
            return Optional.of(identity);
          }
        }
      }
      if (Configuration.containsKey(Template.LOCALITY_TIER_STRICT.format(tier.getTierName()))
          && Configuration.getBoolean(Template.LOCALITY_TIER_STRICT.format(tier.getTierName()))) {
        return Optional.empty();
      }
    }
    return Optional.of(identities.get(0));
  }

  /**
   * @param other a tiered identity to compare to
   * @return whether this tiered identity matches the given tiered identity in all strict tiers
   */
  public boolean strictTiersMatch(TieredIdentity other) {
    for (LocalityTier t : mTiers) {
      PropertyKey strictKey = Template.LOCALITY_TIER_STRICT.format(t.getTierName());
      if (Configuration.containsKey(strictKey) && Configuration.getBoolean(strictKey)) {
        for (LocalityTier tier : other.getTiers()) {
          if (tier.getTierName().equals(t.getTierName()) && !tier.getValue().equals(t.getValue())) {
            return false;
          }
        }
      }
    }
    return true;
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
    return Objects.toStringHelper(this)
        .add("tiers", mTiers)
        .toString();
  }

  /**
   * Class representing a locality tier, e.g. (host: hostname).
   */
  public static final class LocalityTier {
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof LocalityTier)) {
        return false;
      }
      LocalityTier that = (LocalityTier) o;
      return mTierName.equals(that.mTierName)
          && Objects.equal(mValue, that.mValue);
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
