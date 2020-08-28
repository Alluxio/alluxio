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

package alluxio.master.metastore;

/**
 * Options for reading from the inode store.
 */
public class ReadOption {
  private static final ReadOption DEFAULT = new ReadOption(false);

  private final boolean mSkipCache;

  private ReadOption(boolean skipCache) {
    mSkipCache = skipCache;
  }

  /**
   * @return whether to skip caching when reading from the inode store
   */
  public boolean shouldSkipCache() {
    return mSkipCache;
  }

  /**
   * @return a new builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * @return the singleton instance of the default option
   */
  public static ReadOption defaults() {
    return DEFAULT;
  }

  /**
   * Builder for {@link ReadOption}.
   */
  public static class Builder {
    private boolean mSkipCache = false;

    /**
     * Sets whether to skip caching.
     *
     * @param skip skip or not
     * @return the builder
     */
    public Builder setSkipCache(boolean skip) {
      mSkipCache = skip;
      return this;
    }

    /**
     * @return the built option
     */
    public ReadOption build() {
      return new ReadOption(mSkipCache);
    }
  }
}
