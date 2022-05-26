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
 * The options for list bucket operation. (support listObjectV2)
 */
public final class ListBucketOptions {
  // Default number of MaxKeys when it is not specified
  public static final int DEFAULT_MAX_KEYS = 1000;
  // Default EncodingType when it is not specified
  public static final String DEFAULT_ENCODING_TYPE = "url";

  private String mMarker;
  private String mPrefix;
  private int mMaxKeys;
  private String mDelimiter;
  private String mEncodingType;

  private Integer mListType;
  private String mContinuationToken;
  private String mStartAfter;

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
    // common parameter
    mPrefix = "";
    mMaxKeys = DEFAULT_MAX_KEYS;
    mDelimiter = null;
    mEncodingType = DEFAULT_ENCODING_TYPE;
    // listObject parameter
    mMarker = null;
    // listObjectV2 parameter
    mListType = null;
    mContinuationToken = null;
    mStartAfter = null;
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
   * @return the list type
   */
  public Integer getListType() {
    return mListType;
  }

  /**
   * @return the continuation token
   */
  public String getContinuationToken() {
    return mContinuationToken;
  }

  /**
   * @return the start after
   */
  public String getStartAfter() {
    return mStartAfter;
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

  /**
   * @param listType the list type to set
   * @return the updated object
   */
  public ListBucketOptions setListType(Integer listType) {
    mListType = listType;
    return this;
  }

  /**
   * @param continuationToken the continuation token to set
   * @return the updated object
   */
  public ListBucketOptions setContinuationToken(String continuationToken) {
    mContinuationToken = continuationToken;
    return this;
  }

  /**
   * @param startAfter the start after
   * @return the updated object
   */
  public ListBucketOptions setStartAfter(String startAfter) {
    mStartAfter = startAfter;
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
        && Objects.equal(mListType, that.mListType)
        && Objects.equal(mContinuationToken, that.mContinuationToken)
        && Objects.equal(mStartAfter, that.mStartAfter)
        ;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mMarker, mPrefix, mMaxKeys, mDelimiter, mEncodingType,
        mListType, mContinuationToken, mStartAfter);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("marker", mMarker)
        .add("prefix", mPrefix)
        .add("maxKeys", mMaxKeys)
        .add("delimiter", mDelimiter)
        .add("encodingType", mEncodingType)
        .add("listType", mListType)
        .add("continuationToken", mContinuationToken)
        .add("startAfter", mStartAfter)
        .toString();
  }
}
