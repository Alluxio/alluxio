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

package alluxio.underfs.options;

import alluxio.annotation.PublicApi;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for listMultipartUploads in {@link alluxio.underfs.UnderFileSystem}.
 */

@PublicApi
@NotThreadSafe
public class ListMultiPartOptions {

  private String mPrefix;

  private String mDelimiter;

  private String mUploadIdMarker;

  private String mKeyMarker;

  private int mMaxUploads;

  /**
   * @return the default {@link ListMultiPartOptions}
   */
  public static ListMultiPartOptions defaults() {
    return new ListMultiPartOptions();
  }

  /**
   * Constructs a default {@link ListMultiPartOptions}.
   */
  public ListMultiPartOptions() {
    mMaxUploads = 1000;
  }

  /**
   * Gets the optional prefix parameter restricting the response to multipart uploads for keys
   * that begin with the specified prefix.
   * @return prefix
   */
  public String getPrefix() {
    return mPrefix;
  }

  /**
   * Sets the optional prefix parameter restricting the response to multipart uploads for keys
   * that begin with the specified prefix.
   * @param prefix – The optional prefix parameter restricting the response to multipart uploads
   * for keys that begin with the specified prefix.
   */
  public void setPrefix(String prefix) {
    mPrefix = prefix;
  }

  /**
   * Gets the optional delimiter parameter.
   * @return delimiter
   */
  public String getDelimiter() {
    return mDelimiter;
  }

  /**
   * Sets the optional delimiter parameter that causes multipart uploads for keys that contain the
   * same string between the prefix and the first occurrence of the delimiter to be rolled up into
   * a single result element in the CommonPrefixes list.
   * @param delimiter
   */
  public void setDelimiter(String delimiter) {
    mDelimiter = delimiter;
  }

  /**
   * Gets the optional upload ID marker.
   * @return uploadIdMarker
   */
  public String getUploadIdMarker() {
    return mUploadIdMarker;
  }

  /**
   * Sets the optional upload ID marker indicating where in the results to begin listing.
   * @param  uploadIdMarker
   */
  public void setUploadIdMarker(String uploadIdMarker) {
    mUploadIdMarker = uploadIdMarker;
  }

  /**
   * Gets the KeyMarker property for this request.
   * @return keyMarker
   */
  public String getKeyMarker() {
    return mKeyMarker;
  }

  /**
   * Sets the KeyMarker property for this request.
   * @param keyMarker The value that KeyMarker is set to
   */
  public void setKeyMarker(String keyMarker) {
    mKeyMarker = keyMarker;
  }

  /**
   * Gets the optional maximum number of uploads to return.
   * @return maxUploads
   */
  public int getMaxUploads() {
    return mMaxUploads;
  }

  /**
   * Sets the optional maximum number of uploads to return.
   * @param maxUploads
   */
  public void setMaxUploads(int maxUploads) {
    mMaxUploads = maxUploads;
  }
}
