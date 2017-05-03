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

package alluxio;

import com.google.common.collect.ImmutableBiMap;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Creates a two-way mapping between storage tier aliases and ordinal numbers from the given
 * {@link Configuration}.
 */
@ThreadSafe
public abstract class StorageTierAssoc {
  /**
   * An immutable bi-directional mapping between storage level aliases and their ordinals. Immutable
   * maps are thread safe.
   */
  private final ImmutableBiMap<String, Integer> mAliasToOrdinal;

  /**
   * Interprets a tier ordinal given the number of tiers.
   *
   * Non-negative values identify tiers starting from top going down (0 identifies the first tier,
   * 1 identifies the second tier, and so on). If the provided value is greater than the number
   * of tiers, it identifies the last tier. Negative values identify tiers starting from the bottom
   * going up (-1 identifies the last tier, -2 identifies the second to last tier, and so on). If
   * the absolute value of the provided value is greater than the number of tiers, it identifies
   * the first tier.
   *
   * @param ordinal the storage tier ordinal to interpret
   * @param numTiers the number of storage tiers
   * @return a valid tier ordinal
   */
  static int interpretOrdinal(int ordinal, int numTiers) {
    if (ordinal >= 0) {
      return Math.min(ordinal, numTiers - 1);
    }
    return Math.max(numTiers + ordinal, 0);
  }

  /**
   * Constructs a new instance using the given {@link Configuration} object. The mapping cannot be
   * modified after creation.
   *
   * @param levelsProperty the property in the conf that specifies how many levels there are
   * @param template the format for the conf that identifies the alias for each level
   */
  protected StorageTierAssoc(PropertyKey levelsProperty, PropertyKey.Template template) {
    int levels = Configuration.getInt(levelsProperty);
    ImmutableBiMap.Builder<String, Integer> builder = new ImmutableBiMap.Builder<>();
    for (int i = 0; i < levels; i++) {
      String alias = Configuration.get(template.format(i));
      builder.put(alias, i);
    }
    mAliasToOrdinal = builder.build();
  }

  /**
   * Constructs a new instance using the given list of storage tier aliases in order of their
   * position in the hierarchy.
   *
   * @param storageTierAliases the list of aliases
   */
  protected StorageTierAssoc(List<String> storageTierAliases) {
    ImmutableBiMap.Builder<String, Integer> builder = new ImmutableBiMap.Builder<>();
    for (int ordinal = 0; ordinal < storageTierAliases.size(); ordinal++) {
      builder.put(storageTierAliases.get(ordinal), ordinal);
    }
    mAliasToOrdinal = builder.build();
  }

  /**
   * @param ordinal a storage tier ordinal
   * @return the storage tier alias matching the given ordinal
   */
  public String getAlias(int ordinal) {
    return mAliasToOrdinal.inverse().get(interpretOrdinal(ordinal, mAliasToOrdinal.size()));
  }

  /**
   * @param alias a storage tier alias
   * @return the storage tier ordinal matching the given alias
   */
  public int getOrdinal(String alias) {
    return mAliasToOrdinal.get(alias);
  }

  /**
   * @return the size of the alias-ordinal mapping
   */
  public int size() {
    return mAliasToOrdinal.size();
  }

  /**
   * @return a list of storage tier aliases in order of their ordinal value
   */
  public List<String> getOrderedStorageAliases() {
    int size = size();
    List<String> ret = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      ret.add(getAlias(i));
    }
    return ret;
  }
}
