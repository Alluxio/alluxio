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

import com.google.common.base.Preconditions;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A scope encoder that supports encode/decode scope information.
 */
public class ScopeEncoder {
  private final int mMaxNumScopes;
  private final int mScopeMask;
  private final ConcurrentHashMap<CacheScope, Integer> mScopeToId;
  private int mNextId; // the next scope id

  /**
   * Create a scope encoder.
   *
   * @param bitsPerScope the number of bits the scope has
   */
  public ScopeEncoder(int bitsPerScope) {
    Preconditions.checkArgument(bitsPerScope > 0 && bitsPerScope < Integer.SIZE - 1,
            "check the value of bitsPerScope");
    mMaxNumScopes = (1 << bitsPerScope);
    mScopeMask = mMaxNumScopes - 1;
    mNextId = 0;
    mScopeToId = new ConcurrentHashMap<>();
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
          // NOTE: If update mScopeToId ahead of updating mIdToScope,
          // we may read a null scope info in decode.
          int id = mNextId;
          Preconditions.checkArgument(id < mMaxNumScopes, "too many scopes in shadow cache");
          mNextId++;
          mScopeToId.putIfAbsent(scopeInfo, id);
          // if we use mScopeToID.put() here,
          // we will get the findBug's error: the hashmap may not be atomic.
        }
      }
    }
    return mScopeToId.get(scopeInfo) & mScopeMask;
  }
}
