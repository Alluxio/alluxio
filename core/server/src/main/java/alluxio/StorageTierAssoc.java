/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableBiMap;

import alluxio.worker.block.meta.StorageTier;

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
    for (int i = 0; i < levels; i ++) {
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
    for (int ordinal = 0; ordinal < storageTierAliases.size(); ordinal ++) {
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
    for (int i = 0; i < size; i ++) {
      ret.add(getAlias(i));
    }
    return ret;
  }
}
