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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Get bucket result defined in https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html
 * It will be encoded into an XML string to be returned as a response for the REST call.
 */
@JacksonXmlRootElement(localName = "ListBucketResult")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ListBucketResult {
  private static final Logger LOG = LoggerFactory.getLogger(ListBucketResult.class);

  // Name of the bucket
  private String mName;

  /*
   * Returns the number of keys included in the response. The value is always less than or equal
   * to the mMaxKeys value.
   */
  private int mKeyCount;

  // The maximum number of keys returned in the response body.
  private int mMaxKeys;

  private boolean mIsTruncated;

  // Marker is included in the response if it was sent with the request.
  private String mMarker;

  // If only partial results are returned, this value is set as the nextMarker.
  private String mNextMarker;

  // Prefix is included in the response if it was sent with the request.
  private String mPrefix;

  // List of files.
  private List<Content> mContents;

  // List of common prefixes (aka. folders)
  private List<CommonPrefixes> mCommonPrefixes;

  // delimiter used to process keys
  private String mDelimiter;

  // encoding type of params. Usually "url"
  private String mEncodingType;

  /**
   * Creates an {@link ListBucketResult}.
   */
  public ListBucketResult() {}

  /**
   * Creates an {@link ListBucketResult}.
   *
   * @param bucketName the bucket name
   * @param children a list of {@link URIStatus}, representing the objects and common prefixes
   * @param options the list bucket options
   */
  public ListBucketResult(
      String bucketName, List<URIStatus> children, ListBucketOptions options) {
    mName = bucketName;
    mPrefix = options.getPrefix();
    mMarker = options.getMarker();
    mMaxKeys = options.getMaxKeys();
    mDelimiter = options.getDelimiter();
    mEncodingType = options.getEncodingType();

    children.sort(new URIStatusComparator());

    final List<URIStatus> keys = children.stream()
        .filter((status) -> status.getPath().compareTo(mMarker) > 0)
        .limit(mMaxKeys)
        .collect(Collectors.toList());

    final Map<Boolean, List<URIStatus>> typeToStatus = keys.stream()
        .collect(Collectors.groupingBy(URIStatus::isFolder));
    final List<URIStatus> objectsList = typeToStatus.getOrDefault(false, Collections.emptyList());
    final List<URIStatus> prefixList = typeToStatus.getOrDefault(true, Collections.emptyList());

    mContents = new ArrayList<>();
    for (URIStatus status : objectsList) {
      mContents.add(new Content(
          status.getPath().substring(mName.length() + 2), // remove both ends of "/" character
          S3RestUtils.toS3Date(status.getLastModificationTimeMs()),
          String.valueOf(status.getLength())
      ));
    }

    mCommonPrefixes = new ArrayList<>();
    for (URIStatus status : prefixList) {
      final String path = status.getPath();
      // remove both ends of "/" character
      mCommonPrefixes.add(new CommonPrefixes(path.substring(mName.length() + 2) + mDelimiter));
    }

    mKeyCount = mContents.size() + mCommonPrefixes.size();
    mIsTruncated = mKeyCount == mMaxKeys;
    if (mIsTruncated) {
      mNextMarker = keys.get(keys.size() - 1).getPath();
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
   * @return the number of keys included in the response
   */
  @JacksonXmlProperty(localName = "KeyCount")
  public int getKeyCount() {
    return mKeyCount;
  }

  /**
   * @return the number of keys included in the response
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
   * @return the prefix
   */
  @JacksonXmlProperty(localName = "Prefix")
  public String getPrefix() {
    return mPrefix;
  }

  /**
   * @return the delimiter
   */
  @JacksonXmlProperty(localName = "Delimiter")
  public String getDelimiter() {
    return mDelimiter;
  }

  /**
   * @return the encoding type of the result
   */
  @JacksonXmlProperty(localName = "EncodingType")
  public String getEncodingType() {
    return mEncodingType;
  }

  /**
   * @return the marker
   */
  @JacksonXmlProperty(localName = "Marker")
  public String getMarker() {
    return mMarker;
  }

  /**
   * @return the next marker
   */
  @JacksonXmlProperty(localName = "NextMarker")
  public String getNextMarker() {
    return mNextMarker;
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
   * @return the common prefixes
   */
  @JacksonXmlProperty(localName = "CommonPrefixes")
  @JacksonXmlElementWrapper(useWrapping = false)
  public List<CommonPrefixes> getCommonPrefixes() {
    return mCommonPrefixes;
  }

  /**
   * Common Prefixes list placeholder object.
   */
  public class CommonPrefixes {
    private final String mPrefix;

    private CommonPrefixes(String prefix) {
      mPrefix = prefix;
    }

    /**
     * @return the list prefixes
     */
    @JacksonXmlProperty(localName = "Prefix")
    public String getPrefix() {
      return mPrefix;
    }
  }

  /**
   * Object metadata class.
   */
  public class Content {
    /* The object's key. */
    private final String mKey;
    /* Date and time the object was last modified. */
    private final String mLastModified;
    /* Size in bytes of the object. */
    private final String mSize;

    /**
     * Constructs a new {@link Content}.
     *
     * @param key the object key
     * @param lastModified the data and time in string format the object was last modified
     * @param size size in bytes of the object
     */
    public Content(String key, String lastModified, String size) {
      mKey = key;
      mLastModified = lastModified;
      mSize = size;
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
     * @return the size in bytes of the object
     */
    @JacksonXmlProperty(localName = "Size")
    public String getSize() {
      return mSize;
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

