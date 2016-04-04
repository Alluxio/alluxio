/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio;

import alluxio.worker.block.meta.StorageTier;

import com.google.common.collect.ImmutableBiMap;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Creates a two-way mapping between {@link StorageTier} aliases and ordinal numbers from the given
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
   * Constructs a new instance using the given {@link Configuration} object. The mapping cannot be
   * modified after creation.
   *
   * @param conf the Alluxio configuration to build the mapping from
   * @param levelsProperty the property in the conf that specifies how many levels there are
   * @param aliasFormat the format for the conf that identifies the alias for each level
   */
  protected StorageTierAssoc(Configuration conf, String levelsProperty, String aliasFormat) {
    int levels = conf.getInt(levelsProperty);
    ImmutableBiMap.Builder<String, Integer> builder = new ImmutableBiMap.Builder<String, Integer>();
    for (int i = 0; i < levels; i++) {
      String alias = conf.get(String.format(aliasFormat, i));
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
    ImmutableBiMap.Builder<String, Integer> builder = new ImmutableBiMap.Builder<String, Integer>();
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
    return mAliasToOrdinal.inverse().get(ordinal);
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
    List<String> ret = new ArrayList<String>(size);
    for (int i = 0; i < size; i++) {
      ret.add(getAlias(i));
    }
    return ret;
  }
}
