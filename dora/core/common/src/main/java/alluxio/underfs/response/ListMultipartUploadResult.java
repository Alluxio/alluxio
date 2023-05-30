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

package alluxio.underfs.response;

import java.util.List;

/**
 * The result of list multipart upload.
 */
public class ListMultipartUploadResult {

  /**
   * The optional key marker specified in the original request to specify
   * where in the results to begin listing multipart uploads.
   */
  private String mKeyMarker;

  /**
   * The optional delimiter specified in the original request to control how
   * multipart uploads for keys with common prefixes are condensed.
   */
  private String mDelimiter;

  /**
   * The optional prefix specified in the original request to limit the
   * returned multipart uploads to those for keys that match this prefix.
   */
  private String mPrefix;

  /**
   * The optional upload ID marker specified in the original request to
   * specify where in the results to begin listing multipart uploads.
   */
  private String mUploadIdMarker;

  /**
   * The optional maximum number of uploads to be listed, as specified in the
   * original request.
   */
  private int mMaxUploads;

  /**
   * Indicates if the listing is truncated, and additional requests need to be
   * made to get more results.
   */
  private boolean mIsTruncated;

  /**
   * If this listing is truncated, this is the next key marker that should be
   * used in the next request to get the next page of results.
   */
  private String mNextKeyMarker;

  /**
   * If this listing is truncated, this is the next upload ID marker that
   * should be used in the next request to get the next page of results.
   */
  private String mNextUploadIdMarker;

  /**
   * The list of multipart uploads.
   */
  private List<MultipartUploadInfo> mMultipartUploadInfoList;

  /**
   * Constructs a default {@link ListMultipartUploadResult}.
   * @param multipartUploadInfoList
   */
  public ListMultipartUploadResult(List<MultipartUploadInfo> multipartUploadInfoList) {
    mMultipartUploadInfoList = multipartUploadInfoList;
  }

  /**
   * Gets key marker of the list MultipartUpload.
   * @return keyMarker
   */
  public String getKeyMarker() {
    return mKeyMarker;
  }

  /**
   * Sets key marker of the list MultipartUpload.
   * @param keyMarker
   */
  public void setKeyMarker(String keyMarker) {
    mKeyMarker = keyMarker;
  }

  /**
   * Gets delimiter of the list MultipartUpload.
   * @return Delimiter
   */
  public String getDelimiter() {
    return mDelimiter;
  }

  /**
   * Sets delimiter of the list MultipartUpload.
   * @param delimiter
   */
  public void setDelimiter(String delimiter) {
    mDelimiter = delimiter;
  }

  /**
   * Gets prefix of the list MultipartUpload.
   * @return prefix
   */
  public String getPrefix() {
    return mPrefix;
  }

  /**
   * Sets prefix of the list MultipartUpload.
   * @param prefix
   */
  public void setPrefix(String prefix) {
    mPrefix = prefix;
  }

  /**
   * Gets uploadIdMarker of the list MultipartUpload.
   * @return prefix
   */
  public String getUploadIdMarker() {
    return mUploadIdMarker;
  }

  /**
   * Sets uploadIdMarker of the list MultipartUpload.
   * @param uploadIdMarker
   */
  public void setUploadIdMarker(String uploadIdMarker) {
    mUploadIdMarker = uploadIdMarker;
  }

  /**
   * Gets maxUploads of the list MultipartUpload.
   * @return maxUploads
   */
  public int getMaxUploads() {
    return mMaxUploads;
  }

  /**
   * Sets maxUploads of the list MultipartUpload.
   * @param maxUploads
   */
  public void setMaxUploads(int maxUploads) {
    mMaxUploads = maxUploads;
  }

  /**
   * Gets truncated flag of the list MultipartUpload.
   * @return truncated
   */
  public boolean isTruncated() {
    return mIsTruncated;
  }

  /**
   * Sets truncated of the list MultipartUpload.
   * @param truncated
   */
  public void setTruncated(boolean truncated) {
    mIsTruncated = truncated;
  }

  /**
   * Gets nextKeyMarker of the list MultipartUpload.
   * @return nextKeyMarker
   */
  public String getNextKeyMarker() {
    return mNextKeyMarker;
  }

  /**
   * Sets nextKeyMarker of the list MultipartUpload.
   * @param nextKeyMarker
   */
  public void setNextKeyMarker(String nextKeyMarker) {
    mNextKeyMarker = nextKeyMarker;
  }

  /**
   * Gets nextUploadIdMarker of the list MultipartUpload.
   * @return nextUploadIdMarker
   */
  public String getNextUploadIdMarker() {
    return mNextUploadIdMarker;
  }

  /**
   * Sets nextUploadIdMarker of the list MultipartUpload.
   * @param nextUploadIdMarker
   */
  public void setNextUploadIdMarker(String nextUploadIdMarker) {
    mNextUploadIdMarker = nextUploadIdMarker;
  }

  /**
   * Gets MultipartUpload Info List.
   * @return MultipartUpload Info List
   */
  public List<MultipartUploadInfo> getMultipartUploadInfoList() {
    return mMultipartUploadInfoList;
  }
}
