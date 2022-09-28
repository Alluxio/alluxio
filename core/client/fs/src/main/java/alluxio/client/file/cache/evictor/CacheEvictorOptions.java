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

/**
 * Options for initiating cache evictor.
 */
public class CacheEvictorOptions {

  private boolean mIsNondeterministic;
  private Class<? extends CacheEvictor> mEvictorClass;

  /**
   * @return if true, the evictor picks uniformly from the worst k elements
   */
  public boolean isNondeterministic() {
    return mIsNondeterministic;
  }

  /**
   * @return The strategy that client uses to evict local cached pages
   */
  public Class<? extends CacheEvictor> getEvictorClass() {
    return mEvictorClass;
  }

  /**
   * @param isNondeterministic
   * @return CacheEvictorOptions
   */
  public CacheEvictorOptions setIsNondeterministic(boolean isNondeterministic) {
    mIsNondeterministic = isNondeterministic;
    return this;
  }

  /**
   * @param evictorClass
   * @return CacheEvictorOptions
   */
  public CacheEvictorOptions setEvictorClass(Class<? extends CacheEvictor> evictorClass) {
    mEvictorClass = evictorClass;
    return this;
  }
}
