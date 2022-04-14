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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Get bucket result defined in https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html
 * It will be encoded into an XML string to be returned as a response for the REST call.
 */
@JacksonXmlRootElement(localName = "ListBucketResult")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ListBucketResult {
  private static final Logger LOG = LoggerFactory.getLogger(ListBucketResult.class);
  private static final String CONTINUATION_TOKEN_SEPARATOR = "-";

  // Name of the bucket
  private String mName;

  /*
   * Returns the number of keys included in the response. The value is always less than or equal
   * to the mMaxKeys value.
   */
  private int mKeyCount;

  // The maximum number of keys returned in the response body.
  // @JsonInclude: Adding the ALWAYS annotation to overwrite Class-level NON_EMPTY
  @JsonInclude(JsonInclude.Include.ALWAYS)
  private int mMaxKeys;

  // A flag that indicates whether Amazon S3 returned all of the results that satisfied the search
  // criteria. When response is truncated, you can use NextMarker as marker in the subsequent
  // request to get next set of objects.
  private boolean mIsTruncated;

  // Marker is included in the response if it was sent with the request.
  // Otherwise it is set to be an empty string.
  // Note that the Marker does not include the bucket path.
  private String mMarker;

  // If only partial results are returned, this value is set as the nextMarker.
  // This element is returned only if you have delimiter request parameter specified.
  // - https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html
  // Note that the NextMarker does not include the bucket path.
  private String mNextMarker;

  // Prefix is included in the response if it was sent with the request.
  // Otherwise it is set to be an empty string.
  // @JsonInclude: Adding the ALWAYS annotation to overwrite Class-level NON_EMPTY
  @JsonInclude(JsonInclude.Include.ALWAYS)
  private String mPrefix;

  // List of files.
  private List<Content> mContents;

  // List of common prefixes (aka. folders)
  private List<CommonPrefix> mCommonPrefixes;

  // Delimiter string used to group paths by common prefixes
  private String mDelimiter;

  // encoding type of params. Usually "url"
  private String mEncodingType;

  //support listObjectV2
  private Integer mListType;

  // ContinuationToken is included in the response if it was sent with the request.
  private String mContinuationToken;

  /*
   * If only partial results are returned and listType is 2,
   * this value is set as the NextContinuationToken.
   */
  private String mNextContinuationToken;

  // StartAfter is included in the response if it was sent with the request.
  private String mStartAfter;

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
      String bucketName, List<URIStatus> children, ListBucketOptions options) throws S3Exception {
    mName = bucketName;
    mPrefix = options.getPrefix();
    mMarker = options.getMarker();
    mMaxKeys = options.getMaxKeys();
    mDelimiter = options.getDelimiter();
    mEncodingType = options.getEncodingType();

    mListType = options.getListType();
    mContinuationToken = options.getContinuationToken();
    mStartAfter = options.getStartAfter();

    mKeyCount = 0; // explicitly set the starting value of 0
    mIsTruncated = false; // explicitly set the starting value of false
    mNextMarker = null; // explicitly set the starting value of null

    if (mDelimiter != null) {
      mCommonPrefixes = new ArrayList<>();
    }

    // Handle edge-case independently
    if (mMaxKeys == 0) {
      mContents = new ArrayList<>();
      return;
    }
    // contains both ends of "/" character
    final String mNamePrefix = AlluxioURI.SEPARATOR + mName + AlluxioURI.SEPARATOR;
    final List<URIStatus> keys = filterKeysAndBuildCommonPrefixes(mNamePrefix, children);
    buildContents(mNamePrefix, keys);
  }

  /**
   * Filter {@link URIStatus} use marker/continuation-token, prefix, delimiter, and max-keys.
   * @param children a list of {@link URIStatus}, representing the objects and common prefixes
   * @return A list of {@link URIStatus} after filtering
   */
  private List<URIStatus> filterKeysAndBuildCommonPrefixes(
      String bucketPrefix, List<URIStatus> children) throws S3Exception {
    final String marker;
    if (isVersion2()) {
      marker = decodeToken(mContinuationToken);
    } else {
      marker = mMarker;
    }
    //sort use uri path
    children.sort(Comparator.comparing(URIStatus::getPath));
    //group by common prefix if delimiter is provided
    Set<String> commonPrefixes = new HashSet<>();

    List<URIStatus> keys = new ArrayList<>(Math.min(children.size(), mMaxKeys));
    for (URIStatus status : children) {
      String path = status.isFolder() ? status.getPath() + AlluxioURI.SEPARATOR : status.getPath();
      if (path.startsWith(bucketPrefix + mPrefix) //prefix filter
          && path.substring(bucketPrefix.length()).compareTo(marker) > 0 //marker filter
          //startAfter filter for listObjectV2
          && (!isVersion2() || path.compareTo(bucketPrefix + mStartAfter) > 0)) {
        if (mDelimiter == null) {
          if (mKeyCount == mMaxKeys) {
            mIsTruncated = true;
            break;
          }
          mKeyCount += 1;
          keys.add(status);
          continue;
        }
        int delimiterIndex = path.substring(bucketPrefix.length() + mPrefix.length())
            .indexOf(mDelimiter);
        if (delimiterIndex == -1) { // no matching delimiter
          if (mKeyCount == mMaxKeys) {
            mIsTruncated = true;
            break;
          }
          mKeyCount += 1;
          keys.add(status);
          continue;
        }
        /*
         * Delimiter mean:
         * https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html
         * Causes keys that contain the same string between the prefix and the
         * first occurrence of the delimiter to be rolled up into a single
         * result element in the CommonPrefixes collection.
         * These rolled-up keys are not returned elsewhere in the response.
         * Each rolled-up result counts as only one return against the MaxKeys value.
         */
        String commonPrefix = path.substring(bucketPrefix.length(),
            bucketPrefix.length() + mPrefix.length() + delimiterIndex + mDelimiter.length());
        if (commonPrefixes.add(commonPrefix)) {
          if (mKeyCount == mMaxKeys) {
            mIsTruncated = true;
            break;
          }
          mKeyCount += 1; // only increment on new common prefixes
          // NextMarker is not updated on common prefixes
          mCommonPrefixes.add(new CommonPrefix(commonPrefix));
        }
      }
    }
    if (mIsTruncated) {
      // NextMarker is just the last processed Key in the Contents
      mNextMarker = keys.get(keys.size() - 1).getPath().substring(bucketPrefix.length());
      if (isVersion2()) {
        mNextContinuationToken = encodeToken(mNextMarker);
        mNextMarker = "";
      }
    }
    return keys;
  }

  private void buildContents(String bucketPrefix, List<URIStatus> keys) {
    mContents = new ArrayList<>();
    for (URIStatus status : keys) {
      mContents.add(new Content(
          status.isFolder() ? status.getPath().substring(bucketPrefix.length())
              + AlluxioURI.SEPARATOR : status.getPath().substring(bucketPrefix.length()),
          S3RestUtils.toS3Date(status.getLastModificationTimeMs()),
          status.isFolder() ? "0" : String.valueOf(status.getLength())
      ));
    }
  }

  /**
   * @return if listObjectV2 version
   */
  public boolean isVersion2() {
    return this.mListType != null && this.mListType == 2;
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
  @JsonInclude(JsonInclude.Include.ALWAYS)
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
  @JsonInclude(JsonInclude.Include.ALWAYS)
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
   * @return the continuationToken
   */
  @JacksonXmlProperty(localName = "ContinuationToken")
  public String getContinuationToken() {
    return mContinuationToken;
  }

  /**
   * @return the nextContinuationToken
   */
  @JacksonXmlProperty(localName = "NextContinuationToken")
  public String getNextContinuationToken() {
    return mNextContinuationToken;
  }

  /**
   * @return the startAfter
   */
  @JacksonXmlProperty(localName = "StartAfter")
  public String getStartAfter() {
    return mStartAfter;
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
  public List<CommonPrefix> getCommonPrefixes() {
    return mCommonPrefixes;
  }

  /**
   * Generate a continuation token which is used in get Bucket.
   * @param key used to encode to a token
   * @return if key is not null return continuation token, else returns null
   */
  public static String encodeToken(String key) {
    if (key != null && key.length() > 0) {
      byte[] rawLastKey = key.getBytes(StandardCharsets.UTF_8);
      ByteBuffer buffer = ByteBuffer.allocate(rawLastKey.length);
      buffer.put(rawLastKey);
      String hex = Hex.encodeHexString(buffer.array());
      String digest = DigestUtils.sha256Hex(hex);
      return hex + CONTINUATION_TOKEN_SEPARATOR + digest;
    } else {
      return "";
    }
  }

  /**
   * Decode a continuation token which is used in get Bucket.
   * @param token used to decode to a key
   * @return if token is not null return decoded key, otherwise returns null
   * @throws S3Exception
   */
  public static String decodeToken(String token) throws S3Exception {
    if (token != null && token.length() > 0) {
      int indexSeparator = token.indexOf(CONTINUATION_TOKEN_SEPARATOR);
      if (indexSeparator == -1) {
        throw new S3Exception(token, S3ErrorCode.INVALID_CONTINUATION_TOKEN);
      }
      String hex = token.substring(0, indexSeparator);
      String digest = token.substring(indexSeparator + 1);
      String digestActualKey = DigestUtils.sha256Hex(hex);
      if (!digest.equals(digestActualKey)) {
        throw new S3Exception(token, S3ErrorCode.INVALID_CONTINUATION_TOKEN);
      }
      try {
        ByteBuffer buffer = ByteBuffer.wrap(Hex.decodeHex(hex));
        return new String(buffer.array(), StandardCharsets.UTF_8);
      } catch (DecoderException e) {
        throw new S3Exception(e, token, S3ErrorCode.INVALID_CONTINUATION_TOKEN);
      }
    } else {
      return "";
    }
  }

  /**
   * Common Prefixes list placeholder object.
   */
  public static class CommonPrefix {
    private final String mPrefix;

    private CommonPrefix(String prefix) {
      mPrefix = prefix;
    }

    /**
     * @return the list prefixes
     */
    @JacksonXmlProperty(localName = "Prefix")
    public String getPrefix() {
      return mPrefix;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CommonPrefix)) {
        return false;
      }
      CommonPrefix that = (CommonPrefix) o;
      return mPrefix.equals(that.mPrefix);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mPrefix);
    }
  }

  /**
   * Object metadata class.
   */
  public static class Content {
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
}
