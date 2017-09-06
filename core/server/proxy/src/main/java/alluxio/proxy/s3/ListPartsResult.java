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

import alluxio.client.file.URIStatus;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.ArrayList;
import java.util.List;

/**
 * Result returned after requests for listing parts of a multipart upload.
 * It is defined in http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadListParts.html.
 * It will be encoded into an XML string to be returned as a response for the REST call.
 */
// TODO(cc): Support more fields
@JacksonXmlRootElement(localName = "ListPartsResult")
@JsonPropertyOrder({ "Bucket", "Key", "UploadId", "StorageClass", "IsTruncated", "Part" })
public class ListPartsResult {
  /* Name of the bucket. */
  private String mBucket;
  /* Object key. */
  private String mKey;
  /* ID of the multipart upload to be listed. */
  private String mUploadId;
  /* Storage class, only STANDARD is supported in Alluxio now. */
  private String mStorageClass;
  /**
   * Indicates whether the returned list of parts is truncated.
   * A true value indicates that the list was truncated.
   * A list can be truncated if the number of parts exceeds the limit returned in the MaxParts
   * element.
   */
  private boolean mIsTruncated;
  /* Parts of the multipart upload. */
  List<Part> mParts;

  /**
   * Constructs a default {@link ListPartsResult}.
   */
  public ListPartsResult() {
    mBucket = "";
    mKey = "";
    mUploadId = "";
    mStorageClass = S3Constants.S3_STANDARD_STORAGE_CLASS;
    mIsTruncated = false;
    mParts = new ArrayList<>();
  }

  /**
   * @return the bucket name
   */
  @JacksonXmlProperty(localName = "Bucket")
  public String getBucket() {
    return mBucket;
  }

  /**
   * @param bucket the bucket name to set
   */
  @JacksonXmlProperty(localName = "Bucket")
  public void setBucket(String bucket) {
    mBucket = bucket;
  }

  /**
   * @return the object key
   */
  @JacksonXmlProperty(localName = "Key")
  public String getKey() {
    return mKey;
  }

  /**
   * @param key the object key to set
   */
  @JacksonXmlProperty(localName = "Key")
  public void setKey(String key) {
    mKey = key;
  }

  /**
   * @return the upload ID
   */
  @JacksonXmlProperty(localName = "UploadId")
  public String getUploadId() {
    return mUploadId;
  }

  /**
   * @param uploadId the upload ID to set
   */
  @JacksonXmlProperty(localName = "UploadId")
  public void setUploadId(String uploadId) {
    mUploadId = uploadId;
  }

  /**
   * @return the storage class
   */
  @JacksonXmlProperty(localName = "StorageClass")
  public String getStorageClass() {
    return mStorageClass;
  }

  /**
   * @param storageClass the storage class to set
   */
  @JacksonXmlProperty(localName = "StorageClass")
  public void setStorageClass(String storageClass) {
    mStorageClass = storageClass;
  }

  /**
   * @return false if all results are returned, otherwise true
   */
  @JacksonXmlProperty(localName = "IsTruncated")
  public boolean isTruncated() {
    return mIsTruncated;
  }

  /**
   * @param isTruncated whether the results are truncated
   */
  @JacksonXmlProperty(localName = "IsTruncated")
  public void setIsTruncated(boolean isTruncated) {
    mIsTruncated = isTruncated;
  }

  /**
   * @return the list of parts
   */
  @JacksonXmlProperty(localName = "Part")
  @JacksonXmlElementWrapper(useWrapping = false)
  public List<Part> getParts() {
    return mParts;
  }

  /**
   * @param parts the list of parts to set
   */
  @JacksonXmlProperty(localName = "Part")
  @JacksonXmlElementWrapper(useWrapping = false)
  public void setParts(List<Part> parts) {
    mParts = parts;
  }

  /**
   * Part contains metadata of a part of the object in multipart upload.
   */
  @JsonPropertyOrder({ "PartNumber", "LastModified", "ETag", "Size" })
  public static class Part {
    /* Part number. */
    private int mPartNumber;
    /* Last modification time of the part. */
    private String mLastModified;
    /* Entity tag of the part. */
    private String mETag;
    /* Size of the part in bytes. */
    private long mSize;

    /**
     * @param status the status of the part file
     * @return the {@link Part} parsed from the status
     */
    public static Part fromURIStatus(URIStatus status) {
      Part result = new Part();
      result.setPartNumber(Integer.parseInt(status.getName()));
      result.setLastModified(S3RestUtils.toS3Date(status.getLastModificationTimeMs()));
      result.setSize(status.getLength());
      return result;
    }

    /**
     * Constructs a default {@link Part}.
     */
    public Part() {
      mPartNumber = 0;
      mLastModified = "";
      mETag = S3RestUtils.quoteETag("");
      mSize = 0;
    }

    /**
     * @return the part number
     */
    @JacksonXmlProperty(localName = "PartNumber")
    public int getPartNumber() {
      return mPartNumber;
    }

    /**
     * @param partNumber the part number to set
     */
    @JacksonXmlProperty(localName = "PartNumber")
    public void setPartNumber(int partNumber) {
      mPartNumber = partNumber;
    }

    /**
     * @return the last modification time
     */
    @JacksonXmlProperty(localName = "LastModified")
    public String getLastModified() {
      return mLastModified;
    }

    /**
     * @param lastModified the last modification time to set
     */
    @JacksonXmlProperty(localName = "LastModified")
    public void setLastModified(String lastModified) {
      mLastModified = lastModified;
    }

    /**
     * @return the entity tag surrounded by quotes
     */
    @JacksonXmlProperty(localName = "ETag")
    public String getETag() {
      return mETag;
    }

    /**
     * @param etag the entity tag to set (without surrounding quotes)
     */
    @JacksonXmlProperty(localName = "ETag")
    public void setETag(String etag) {
      mETag = S3RestUtils.quoteETag(etag);
    }

    /**
     * @return the size of the part in bytes
     */
    @JacksonXmlProperty(localName = "Size")
    public long getSize() {
      return mSize;
    }

    /**
     * @param size the size of the part (in bytes) to set
     */
    @JacksonXmlProperty(localName = "Size")
    public void setSize(long size) {
      mSize = size;
    }
  }
}
