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

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.wire.FileInfo;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Get bucket result defined in http://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html
 * It will be encoded into an XML string to be returned as a response for the REST call.
 *
 * TODO(chaomin): consider add more required fields in S3 BucketGet API.
 */
@JacksonXmlRootElement(localName = "ListBucketResult")
@JsonPropertyOrder({ "Name", "Prefix", "ContinuationToken", "NextContinuationToken",
    "KeyCount", "MaxKeys", "IsTruncated", "Contents" })
public class ListBucketResult {

  /* Name of the bucket. */
  private String mName = "";
  /* Keys that begin with the indicated prefix. */
  private String mPrefix = null;
  /* ContinuationToken is included in the response if it was sent with the request. */
  private String mContinuationToken = null;
  /**
   * Returns the number of keys included in the response. The value is always less than or equal
   * to the mMaxKeys value.
   */
  private int mKeyCount = 0;
  /* The maximum number of keys returned in the response body. */
  private int mMaxKeys = S3Constants.S3_DEFAULT_MAX_KEYS;
  /**
   * Specifies whether or not all of the results were returned. If the number of
   * results exceeds that specified by MaxKeys, all of the results might not be returned.
   */
  private boolean mIsTruncated = false;
  /**
   * If only partial results are returned (i.e. mIsTruncated = true), this value is set as the
   * next continuation token. The continuation token is the full Alluxio path of the object.
   */
  private String mNextContinuationToken = null;
  /* A list of objects with metadata. */
  private List<Content> mContents = new ArrayList<>();

  /**
   * Creates an {@link ListBucketResult}.
   */
  public ListBucketResult() {}

  /**
   * Creates an {@link ListBucketResult}.
   *
   * @param bucketName the bucket name
   * @param objectsList a list of {@link URIStatus}, representing the objects
   * @param options the list bucket options
   */
  public ListBucketResult(
      String bucketName, List<URIStatus> objectsList, ListBucketOptions options) {
    mName = bucketName;
    mPrefix = options.getPrefix();
    mKeyCount = 0;
    if (options.getMaxKeys() != null) {
      mMaxKeys = Integer.parseInt(options.getMaxKeys());
    }
    mContents = new ArrayList<>();
    mContinuationToken = options.getContinuationToken();

    Collections.sort(objectsList, new URIStatusComparator());

    int startIndex = 0;
    if (options.getContinuationToken() != null) {
      URIStatus tokenStatus = new URIStatus(new FileInfo().setPath(
          mName + AlluxioURI.SEPARATOR + mContinuationToken));
      startIndex = Collections.binarySearch(objectsList, tokenStatus, new URIStatusComparator());
      if (startIndex < 0) {
        // If continuation token does not exist in the object list, find the first element which is
        // greater than the token.
        startIndex = (-1) * startIndex - 1;
      }
    }

    for (int i = startIndex; i < objectsList.size(); i++) {
      URIStatus status = objectsList.get(i);
      String objectKey = status.getPath().substring(mName.length() + 1);
      if (mKeyCount >= mMaxKeys) {
        mIsTruncated = true;
        mNextContinuationToken = objectKey;
        return;
      }
      if (mContinuationToken != null && objectKey.compareTo(mContinuationToken) < 0) {
        continue;
      }
      if (mPrefix != null && !objectKey.startsWith(mPrefix)) {
        continue;
      }
      // TODO(chaomin): set ETag once there's a way to get MD5 hash of an Alluxio file.
      // TODO(chaomin): construct the response with CommonPrefixes when delimiter support is added.
      mContents.add(new Content(
          status.getPath().substring(mName.length() + 1),
          S3RestUtils.toS3Date(status.getLastModificationTimeMs()),
          S3Constants.S3_EMPTY_ETAG,
          String.valueOf(status.getLength()),
          S3Constants.S3_STANDARD_STORAGE_CLASS));
      mKeyCount++;
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
   * @return the continuation token
   */
  @JacksonXmlProperty(localName = "ContinuationToken")
  public String getContinuationToken() {
    return mContinuationToken;
  }

  /**
   * @return the next continuation token
   */
  @JacksonXmlProperty(localName = "NextContinuationToken")
  public String getNextContinuationToken() {
    return mNextContinuationToken;
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
   * @return false if all results are returned, otherwise true
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
   * @param continuationToken the continuation token to set
   */
  @JacksonXmlProperty(localName = "ContinuationToken")
  public void setContinuationToken(String continuationToken) {
    mContinuationToken = continuationToken;
  }

  /**
   * @param nextContinuationToken the next continuation token to set
   */
  @JacksonXmlProperty(localName = "NextContinuationToken")
  public void setNextContinuationToken(String nextContinuationToken) {
    mNextContinuationToken = nextContinuationToken;
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
    /* The object's key. */
    private String mKey;
    /* Date and time the object was last modified. */
    private String mLastModified;
    /* The entity tag is an MD5 hash of the object. */
    // TODO(chaomin): for now this is always empty because file content's MD5 hash is not yet
    // part of Alluxio metadata
    private String mETag;
    /* Size in bytes of the object. */
    private String mSize;
    /**
     * Storage class (STANDARD | STANDARD_IA | REDUCED_REDUNDANCY | GLACIER). Alluxio only supports
     * STANDARD for now.
     */
    private String mStorageClass;

    /**
     * Constructs a new {@link Content}.
     */
    public Content() {
      mKey = "";
      mLastModified = "";
      mETag = "";
      mSize = "";
      mStorageClass = S3Constants.S3_STANDARD_STORAGE_CLASS;
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

  private class URIStatusComparator implements Comparator<URIStatus> {
    @Override
    public int compare(URIStatus o1, URIStatus o2) {
      return o1.getPath().compareTo(o2.getPath());
    }

    /**
     * Constructs a new {@link URIStatusComparator}.
     */
    public URIStatusComparator() {}
  }
}

