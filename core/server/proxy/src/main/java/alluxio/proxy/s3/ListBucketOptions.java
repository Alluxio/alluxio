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

package alluxio.proxy.s3;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * The options for list bucket operation.
 */
public final class ListBucketOptions {
  // Default number of MaxKeys when it is not specified
  public static final int DEFAULT_MAX_KEYS = 1000;

  private String mMarker;
  private String mPrefix;
  private int mMaxKeys;
  private String mDelimiter;
  private String mEncodingType;

  /**
   * Creates a default {@link ListBucketOptions}.
   *
   * @return the created {@link ListBucketOptions}
   */
  public static ListBucketOptions defaults() {
    return new ListBucketOptions();
  }

  /**
   * Constructs a new {@link ListBucketOptions}.
   */
  private ListBucketOptions() {
    mMarker = "";
    mPrefix = "";
    mMaxKeys = DEFAULT_MAX_KEYS;
    mDelimiter = "/";
    mEncodingType = "url";
  }

  /**
   * @return the marker
   */
  public String getMarker() {
    return mMarker;
  }

  /**
   * @return the prefix
   */
  public String getPrefix() {
    return mPrefix;
  }

  /**
   * @return the max keys
   */
  public int getMaxKeys() {
    return mMaxKeys;
  }

  /**
   * @return the delimiter
   */
  public String getDelimiter() {
    return mDelimiter;
  }

  /**
   * @return the encoding type
   */
  public String getEncodingType() {
    return mEncodingType;
  }

  /**
   * @param marker the marker to set
   * @return the updated object
   */
  public ListBucketOptions setMarker(String marker) {
    mMarker = marker;
    return this;
  }

  /**
   * @param prefix the prefix to set
   * @return the updated object
   */
  public ListBucketOptions setPrefix(String prefix) {
    mPrefix = prefix;
    return this;
  }

  /**
   * @param maxKeys the max keys
   * @return the updated object
   */
  public ListBucketOptions setMaxKeys(int maxKeys) {
    mMaxKeys = maxKeys;
    return this;
  }

  /**
   * @param delimiter the delimiter
   * @return the updated object
   */
  public ListBucketOptions setDelimiter(String delimiter) {
    mDelimiter = delimiter;
    return this;
  }

  /**
   * @param encodingType the encoding type
   * @return the updated object
   */
  public ListBucketOptions setEncodingType(String encodingType) {
    mEncodingType = encodingType;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ListBucketOptions)) {
      return false;
    }
    ListBucketOptions that = (ListBucketOptions) o;
    return Objects.equal(mMarker, that.mMarker)
        && Objects.equal(mPrefix, that.mPrefix)
        && Objects.equal(mMaxKeys, that.mMaxKeys)
        && Objects.equal(mDelimiter, that.mDelimiter)
        && Objects.equal(mEncodingType, that.mEncodingType)
        ;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mMarker, mPrefix, mMaxKeys, mDelimiter, mEncodingType);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("marker", mMarker)
        .add("prefix", mPrefix)
        .add("maxKeys", mMaxKeys)
        .add("delimiter", mDelimiter)
        .add("encodingType", mEncodingType)
        .toString();
  }
}
