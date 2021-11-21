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

package alluxio.client.file.cache.cuckoofilter;

import alluxio.annotation.SuppressFBWarnings;
import alluxio.client.quota.CacheScope;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A scope encoder that supports encode/decode scope information.
 */
public class ScopeEncoder {
  private final int mMaxNumScopes;
  private final int mScopeMask;
  private final ConcurrentHashMap<CacheScope, Integer> mScopeToId;
  private final ConcurrentHashMap<Integer, CacheScope> mIdToScope;
  private int mCount; // the next scope id

  /**
   * Create a scope encoder.
   *
   * @param bitsPerScope the number of bits the scope has
   */
  public ScopeEncoder(int bitsPerScope) {
    mMaxNumScopes = (1 << bitsPerScope);
    mScopeMask = mMaxNumScopes - 1;
    mCount = 0;
    mScopeToId = new ConcurrentHashMap<>();
    mIdToScope = new ConcurrentHashMap<>();
  }

  /**
   * Encode scope information into integer.
   *
   * @param scopeInfo the scope will be encoded
   * @return the encoded scope
   */
  @SuppressFBWarnings(value = "RV_RETURN_VALUE_OF_PUTIFABSENT_IGNORED")
  public int encode(CacheScope scopeInfo) {
    if (!mScopeToId.containsKey(scopeInfo)) {
      synchronized (this) {
        if (!mScopeToId.containsKey(scopeInfo)) {
          // TODO(iluoeli): make sure scope id is smaller than mMaxNumScopes
          // NOTE: If update mScopeToId ahead of updating mIdToScope,
          // we may read a null scope info in decode.
          int id = mCount;
          mCount++;
          mIdToScope.putIfAbsent(id, scopeInfo);
          mScopeToId.putIfAbsent(scopeInfo, id);
        }
      }
    }
    return mScopeToId.get(scopeInfo) & mScopeMask;
  }

  /**
   * Decode scope information from integer.
   *
   * @param id the encoded scope id will be decoded
   * @return the decoded scope information
   */
  public CacheScope decode(int id) {
    return mIdToScope.get(id);
  }
}
