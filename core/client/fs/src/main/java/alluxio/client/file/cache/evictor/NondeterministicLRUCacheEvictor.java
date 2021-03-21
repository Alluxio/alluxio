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

package alluxio.client.file.cache.evictor;

import alluxio.client.file.cache.PageId;
import alluxio.conf.AlluxioConfiguration;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * LRU with non-deterministic cache eviction policy. Evict uniformly from the last mNumOfCandidate
 * of elements at the LRU tail.
 */
@ThreadSafe
public class NondeterministicLRUCacheEvictor extends LRUCacheEvictor {
  // TODO(zhenyu): is 16 the best default number?
  private int mNumOfCandidate = 16;

  /**
   * Required constructor.
   *
   * @param conf Alluxio configuration
   */
  public NondeterministicLRUCacheEvictor(AlluxioConfiguration conf) {
    super(conf);
  }

  /**
   * @param n Number of eviction candidate to randomly select from
   */
  public void setNumOfCandidate(int n) {
    mNumOfCandidate = n;
  }

  @Nullable
  @Override
  public PageId evict() {
    synchronized (mLRUCache) {
      if (mLRUCache.isEmpty()) {
        return null;
      }
      Iterator<PageId> it = mLRUCache.keySet().iterator();
      PageId evictionCandidate = it.next();
      int numMoveFromTail = ThreadLocalRandom.current().nextInt(mNumOfCandidate);
      for (int i = 0; it.hasNext() && i < numMoveFromTail; ++i) {
        evictionCandidate = it.next();
      }
      return evictionCandidate;
    }
  }
}
