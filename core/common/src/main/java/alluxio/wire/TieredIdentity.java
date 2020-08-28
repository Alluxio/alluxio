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

import alluxio.annotation.PublicApi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import java.util.List;
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
  @ApiModelProperty(value = "Tiers included in the tier identity")
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
    @ApiModelProperty(value = "Name of the tier", example = "host")
    public String getTierName() {
      return mTierName;
    }

    /**
     * @return the value
     */
    @Nullable
    @ApiModelProperty(value = "Value of the tier name", example = "localhost")
    public String getValue() {
      return mValue;
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
      return MoreObjects.toStringHelper(this)
          .add("tierName", mTierName)
          .add("value", mValue)
          .toString();
    }
  }
}
