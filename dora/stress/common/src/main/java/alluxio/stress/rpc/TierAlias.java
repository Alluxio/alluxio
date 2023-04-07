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

package alluxio.stress.rpc;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Aliases for storage tiers.
 */
public enum TierAlias {
  MEM("MEM"), SSD("SSD"), HDD("HDD");

  public static final List<TierAlias> SORTED = ImmutableList.of(MEM, SSD, HDD);
  private final String mAlias;

  TierAlias(String alias) {
    mAlias = alias;
  }

  /**
   * Gets the order of this alias as defined in {@link #SORTED}.
   * @return order of the alias
   */
  public int getOrder() {
    return SORTED.indexOf(this);
  }

  @Override
  public String toString() {
    return mAlias;
  }
}
