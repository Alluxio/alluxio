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

import javax.annotation.Nullable;

/**
 * Options for reading from the inode store.
 */
public class ReadOption {
  private static final ReadOption DEFAULT = new ReadOption(false, null, null);

  private final boolean mSkipCache;
  private final String mStartFrom;
  private final String mPrefix;

  private ReadOption(boolean skipCache, String readFrom, String prefix) {
    mSkipCache = skipCache;
    mStartFrom = readFrom;
    mPrefix = prefix;
  }

  /**
   * @return whether to skip caching when reading from the inode store
   */
  public boolean shouldSkipCache() {
    return mSkipCache;
  }

  /**
   * @return path from where to start traversing the list of children from, or null
   * if traversal should start from the beginning
   */
  public @Nullable String getStartFrom() { return mStartFrom; }

  /**
   * @return prefix to filter children path names from
   */
  public @Nullable String getPrefix() { return mPrefix; }

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
    private String mReadFrom = null;
    private String mPrefix = null;

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
     * Set the path where to start traversing the list of children from.
     * @param readFrom the path where to start traversing
     * @return the builder
     */
    public Builder setReadFrom(String readFrom) {
      mReadFrom = readFrom;
      return this;
    }

    /**
     * Set the prefix of the path to filter children by.
     * @param prefix the path prefix
     * @return the builder
     */
    public Builder setPrefix(String prefix) {
      mPrefix = prefix;
      return this;
    }

    /**
     * @return the built option
     */
    public ReadOption build() {
      return new ReadOption(mSkipCache, mReadFrom, mPrefix);
    }
  }
}
