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

/**
 * PartSummaryInfo contains the metadata of a single part of the object in multipart upload.
 */
public class PartSummaryInfo {
  /* Part number. */
  private int mPartNumber;
  /* Last modification time of the part. */
  private String mLastModified;
  /* Entity tag of the part. */
  private String mETag;
  /* Size of the part in bytes. */
  private long mSize;

  /**
   * Constructs a default {@link PartSummaryInfo}.
   * @param partNumber the part number of the part
   * @param lastModified the last modified time
   * @param etag the etag of the part
   * @param size the size of the part
   */
  public PartSummaryInfo(int partNumber, String lastModified, String etag, long size) {
    mPartNumber = partNumber;
    mLastModified = lastModified;
    mETag = etag;
    mSize = size;
  }

  /**
   * @return the part number
   */
  public int getPartNumber() {
    return mPartNumber;
  }

  /**
   * @param partNumber the part number to set
   */
  public void setPartNumber(int partNumber) {
    mPartNumber = partNumber;
  }

  /**
   * @return the last modification time
   */
  public String getLastModified() {
    return mLastModified;
  }

  /**
   * @param lastModified the last modification time to set
   */
  public void setLastModified(String lastModified) {
    mLastModified = lastModified;
  }

  /**
   * @return the entity tag
   */
  public String getETag() {
    return mETag;
  }

  /**
   * @param etag the entity tag to set
   */
  public void setETag(String etag) {
    mETag = etag;
  }

  /**
   * @return the size of the part in bytes
   */
  public long getSize() {
    return mSize;
  }

  /**
   * @param size the size of the part (in bytes) to set
   */
  public void setSize(long size) {
    mSize = size;
  }
}
