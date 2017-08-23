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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

/**
 * Get bucket result defined in http://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html
 * It will be encoded into an XML string to be returned as an error response for the REST call.
 *
 * TODO(chaomin): consider add more required fields in S3 BucketGet API.
 */
@JacksonXmlRootElement(localName = "ListBucketResult")
@JsonPropertyOrder(
    { "Name", "Prefix", "KeyCount", "MaxKeys", "IsTruncated", "Contents" })
public class ListBucketResult {
  // Name of the bucket.
  private String mName = "";
  // Keys that begin with the indicated prefix.
  private String mPrefix = null;
  // Returns the number of keys included in the response. The value is always less than or equal
  // to the mMaxKeys value.
  private int mKeyCount = 0;
  // The maximum number of keys returned in the response body.
  private int mMaxKeys = 0;
  // Specifies whether or not all of the results were returned. If the number of
  // results exceeds that specified by MaxKeys, all of the results might not be returned.
  // TODO(chaomin): set mIsTruncated once Alluxio supports maxKeys and partial result in response.
  private boolean mIsTruncated = false;
  // A list of objects with metadata.
  private List<Content> mContents = new ArrayList<>();

  /**
   * Creates an {@link ListBucketResult}.
   */
  public ListBucketResult() {}

  /**
   * Creates an {@link ListBucketResult}.
   *
   * @param bucketName the bucket name
   * @param listStatusResult a list of {@link URIStatus}, the result of Alluxio listStatus
   */
  public ListBucketResult(String bucketName, List<URIStatus> listStatusResult) {
    mName = bucketName;
    mKeyCount = listStatusResult.size();
    mMaxKeys = listStatusResult.size();
    mContents = new ArrayList<>();
    listStatusResult.sort(new Comparator<URIStatus>() {
      @Override
      public int compare(URIStatus o1, URIStatus o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    // TODO(chaomin): support setting max-keys in the request param.
    for (URIStatus status : listStatusResult) {
      // TODO(chaomin): set ETag once there's a way to get MD5 hash of an Alluxio file.
      mContents.add(new Content(
          status.getName(),
          format.format(new Date(status.getLastModificationTimeMs())),
          "",
          String.valueOf(status.getLength()),
          "STANDARD"));
    }
  }

  /**
   * @return the bucket name
   */
  @JacksonXmlProperty(localName = "Name")
  public String getName() {
    return mName;
  }

  /**
   * @return the prefix
   */
  @JacksonXmlProperty(localName = "Prefix")
  public String getPrefix() {
    return mPrefix;
  }

  /**
   * @return the number of keys included in the response
   */
  @JacksonXmlProperty(localName = "KeyCount")
  public int getKeyCount() {
    return mKeyCount;
  }

  /**
   * @return the maximum number of keys returned in the response body
   */
  @JacksonXmlProperty(localName = "MaxKeys")
  public int getMaxKeys() {
    return mMaxKeys;
  }

  /**
   * @return whether (true) or not (false) all of the results were returned
   */
  @JacksonXmlProperty(localName = "IsTruncated")
  public boolean isTruncated() {
    return mIsTruncated;
  }

  /**
   * @return the list of contents
   */
  @JacksonXmlProperty(localName = "Contents")
  @JacksonXmlElementWrapper(useWrapping = false)
  public List<Content> getContents() {
    return mContents;
  }

  /**
   * @param name the bucket name to set
   */
  @JacksonXmlProperty(localName = "Name")
  public void setName(String name) {
    mName = name;
  }

  /**
   * @param prefix the prefix to set
   */
  @JacksonXmlProperty(localName = "Prefix")
  public void setPrefix(String prefix) {
    mPrefix = prefix;
  }

  /**
   * @param keyCount the number of keys to set
   */
  @JacksonXmlProperty(localName = "KeyCount")
  public void setKeyCount(int keyCount) {
    mKeyCount = keyCount;
  }

  /**
   * @param maxKeys the maximum number of keys returned value to set
   */
  @JacksonXmlProperty(localName = "MaxKeys")
  public void setMaxKeys(int maxKeys) {
    mMaxKeys = maxKeys;
  }

  /**
   * @param isTruncated the isTruncated value to set
   */
  @JacksonXmlProperty(localName = "IsTruncated")
  public void setIsTruncated(boolean isTruncated) {
    mIsTruncated = isTruncated;
  }

  /**
   * @param contents the contents list to set
   */
  @JacksonXmlProperty(localName = "Contents")
  @JacksonXmlElementWrapper(useWrapping = false)
  public void setContent(List<Content> contents) {
    mContents = contents;
  }

  /**
   * Object metadata class.
   */
  @JsonPropertyOrder({ "Key", "LastModified", "ETag", "Size", "StorageClass" })
  public class Content {
    // The object's key.
    private String mKey;
    // Date and time the object was last modified.
    private String mLastModified;
    // The entity tag is an MD5 hash of the object.
    // TODO(chaomin): for now this is always empty because file content's MD5 hash is not yet
    // part of Alluxio metadata
    private String mETag;
    // Size in bytes of the object.
    private String mSize;
    // Storage class (STANDARD | STANDARD_IA | REDUCED_REDUNDANCY | GLACIER). Alluxio only supports
    // STANDARD for now.
    private String mStorageClass;

    /**
     * Constructs a new {@link Content}.
     */
    public Content() {
      mKey = "";
      mLastModified = "";
      mETag = "";
      mSize = "";
      mStorageClass = "STANDARD";
    }

    /**
     * Constructs a new {@link Content}.
     *
     * @param key the object key
     * @param lastModified the data and time in string format the object was last modified
     * @param eTag the entity tag (MD5 hash of the object contents)
     * @param size size in bytes of the object
     * @param storageClass storage class
     */
    public Content(String key, String lastModified, String eTag, String size, String storageClass) {
      mKey = key;
      mLastModified = lastModified;
      mETag = eTag;
      mSize = size;
      mStorageClass = storageClass;
    }

    /**
     * @return the object key
     */
    @JacksonXmlProperty(localName = "Key")
    public String getKey() {
      return mKey;
    }

    /**
     * @return the last modified date and time in string format
     */
    @JacksonXmlProperty(localName = "LastModified")
    public String getLastModified() {
      return mLastModified;
    }

    /**
     * @return the ETag
     */
    @JacksonXmlProperty(localName = "ETag")
    public String getETag() {
      return mETag;
    }

    /**
     * @return the size in bytes of the object
     */
    @JacksonXmlProperty(localName = "Size")
    public String getSize() {
      return mSize;
    }

    /**
     * @return the storage class
     */
    @JacksonXmlProperty(localName = "StorageClass")
    public String getStorageClass() {
      return mStorageClass;
    }

    /**
     * @param key the object key to set
     */
    @JacksonXmlProperty(localName = "Key")
    public void setKey(String key) {
      mKey = key;
    }

    /**
     * @param lastModified the last modified time to set
     */
    @JacksonXmlProperty(localName = "LastModified")
    public void setLastModified(String lastModified) {
      mLastModified = lastModified;
    }

    /**
     * @param eTag the ETag to set
     */
    @JacksonXmlProperty(localName = "ETag")
    public void setETag(String eTag) {
      mETag = eTag;
    }

    /**
     * @param size the object size in bytes to set
     */
    @JacksonXmlProperty(localName = "Size")
    public void setSize(String size) {
      mSize = size;
    }

    /**
     * @param storageClass the storage class to set
     */
    @JacksonXmlProperty(localName = "StorageClass")
    public void setStorageClass(String storageClass) {
      mStorageClass = storageClass;
    }
  }
}

