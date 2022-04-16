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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.xml.bind.annotation.XmlTransient;

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
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Integer mKeyCount;

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
  @JsonInclude(JsonInclude.Include.NON_NULL)
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
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private List<CommonPrefix> mCommonPrefixes;

  // Delimiter string used to group paths by common prefixes
  private String mDelimiter;

  // encoding type of params. Usually "url"
  private String mEncodingType;

  //support listObjectV2
  @XmlTransient
  private Integer mListType;

  // ContinuationToken is included in the response if it was sent with the request.
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String mContinuationToken;

  /*
   * If only partial results are returned and listType is 2,
   * this value is set as the NextContinuationToken.
   */
  private String mNextContinuationToken;

  // StartAfter is included in the response if it was sent with the request.
  @JsonInclude(JsonInclude.Include.NON_NULL)
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
    if (mName == null || mName.isEmpty()) {
      throw new S3Exception(S3ErrorCode.INVALID_BUCKET_NAME);
    }
    mPrefix = options.getPrefix() == null ? "" : options.getPrefix();
    mContents = new ArrayList<>();
    mIsTruncated = false; // explicitly set the starting value of false
    mMaxKeys = options.getMaxKeys();
    if (mMaxKeys < 0) {
      throw new S3Exception(new S3ErrorCode(
          S3ErrorCode.INVALID_ARGUMENT.getCode(),
          "MaxKeys may not be negative",
          S3ErrorCode.INVALID_ARGUMENT.getStatus()));
    }
    mDelimiter = options.getDelimiter();
    if (mDelimiter != null) {
      mCommonPrefixes = new ArrayList<>();
    } // otherwise, mCommonPrefixes is null
    mEncodingType = options.getEncodingType() == null ? ListBucketOptions.DEFAULT_ENCODING_TYPE
        : options.getEncodingType();

    mListType = options.getListType();
    if (mListType == null) { // ListObjects v1
      // Providing a null Marker still results in an empty value being returned,
      // so we set the default value to an empty string
      mMarker = options.getMarker() == null ? "" : options.getMarker();
      mNextMarker = null; // explicitly set the starting value of null
    } else { // ListObjectsV2
      mKeyCount = null; // explicitly set the starting value of null
      mContinuationToken = options.getContinuationToken();
      mNextContinuationToken = null; // explicitly set the starting value of null
      mStartAfter = options.getStartAfter();
    }

    if (mMaxKeys == 0) {
      return;
    }
    // contains both ends of "/" character
    final String mBucketPrefix = AlluxioURI.SEPARATOR + mName + AlluxioURI.SEPARATOR;
    buildListBucketResult(mBucketPrefix, children);
  }

  /**
   * Filter {@link URIStatus} use marker/continuation-token, prefix, delimiter, and max-keys.
   * @param children a list of {@link URIStatus}, representing the objects and common prefixes
   */
  private void buildListBucketResult(
      String bucketPrefix, List<URIStatus> children) throws S3Exception {
    final String marker;
    if (isVersion2()) {
      if (mContinuationToken != null) {
        marker = decodeToken(mContinuationToken);
      } else {
        marker = "";
      }
    } else {
      marker = mMarker;
    }

    //group by common prefix if delimiter is provided
    Set<String> commonPrefixes = new HashSet<>();
    // used when handling truncating
    AtomicReference<String> priorNextMarker = new AtomicReference<>();

    //sort use uri path
    children.sort(Comparator.comparing(URIStatus::getPath));
    mContents = children.stream()
        //marker filter
        .filter(status -> {
          String path = status.getPath().substring(bucketPrefix.length());
          return (path.startsWith(mPrefix) //prefix filter
              && path.compareTo(marker) > 0 //marker filter
              //startAfter filter for listObjectV2
              && (!isVersion2() || mStartAfter == null
                || path.compareTo(mStartAfter) > 0));
        })
        .map(status -> {
          String path = status.getPath().substring(bucketPrefix.length());
          return new Content(
              status.isFolder() ? path + AlluxioURI.SEPARATOR : path,
              S3RestUtils.toS3Date(status.getLastModificationTimeMs()),
              status.isFolder() ? "0" : String.valueOf(status.getLength())
          );
        })
        .filter(content -> {
          String path = content.getKey();
          if (mDelimiter == null) {
            priorNextMarker.set(mNextMarker);
            mNextMarker = path;
            return true;
          }
          int delimiterIndex = path.substring(mPrefix.length()).indexOf(mDelimiter);
          if (delimiterIndex == -1) { // no matching delimiter
            priorNextMarker.set(mNextMarker);
            mNextMarker = path;
            return true;
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
          String commonPrefix = path.substring(0, mPrefix.length() + delimiterIndex
              + mDelimiter.length());
          if (commonPrefix.equals(marker)) {
            return false; // skip if the marker was this common prefix
          }
          if (commonPrefixes.add(commonPrefix)) {
            mCommonPrefixes.add(new CommonPrefix(commonPrefix));
            priorNextMarker.set(mNextMarker);
            mNextMarker = commonPrefix;
            return true; // we will add the common prefix to the key stream for processing purposes
          }
          return false; // the key is dropped because it is consumed by the common prefix
        })
        .limit(mMaxKeys + 1) // limit to +1 in order to check if we have exactly MaxKeys or not
        .filter(content -> {
          // Filter out the common prefixes from the keys
          String path = content.getKey();
          if (mDelimiter == null) {
            return true;
          }
          int delimiterIndex = path.substring(mPrefix.length()).indexOf(mDelimiter);
          if (delimiterIndex == -1) { // no matching delimiter
            return true; // include keys which are not common prefixes
          }
          String commonPrefix = path.substring(0, mPrefix.length() + delimiterIndex
              + mDelimiter.length());
          return !commonPrefixes.contains(commonPrefix); // filter out common prefixes
        })
        .collect(Collectors.toList());

    // Check and populate truncation fields
    if (mContents.size() + (mCommonPrefixes == null ? 0 : mCommonPrefixes.size()) == mMaxKeys + 1) {
      mIsTruncated = true;
      // Handle edge-case for truncating vs having <= MaxKeys
      if (mContents.get(mContents.size() - 1).getKey().equals(mNextMarker)) {
        mContents.remove(mContents.size() - 1);
      } else if (mCommonPrefixes != null
          && mCommonPrefixes.get(mCommonPrefixes.size() - 1).getPrefix().equals(mNextMarker)) {
        mCommonPrefixes.remove(mCommonPrefixes.size() - 1);
      } else {
        throw new S3Exception(new S3ErrorCode(
            S3ErrorCode.INTERNAL_ERROR.getCode(),
            "Failed to populate ListBucketResult",
            S3ErrorCode.INTERNAL_ERROR.getStatus()
        ));
      }

      mNextMarker = priorNextMarker.get(); // nullable
      if (isVersion2() && mNextMarker != null) {
        mNextContinuationToken = encodeToken(mNextMarker);
        mNextMarker = null;
      }
    } else {
      mNextContinuationToken = null;
      mNextMarker = null;
    }
    if (isVersion2()) {
      mKeyCount = mContents.size() + (mCommonPrefixes == null ? 0 : mCommonPrefixes.size());
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
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JacksonXmlProperty(localName = "KeyCount")
  public Integer getKeyCount() {
    return mKeyCount;
  }

  /**
   * @return the number of keys included in the response
   */
  @JsonInclude(JsonInclude.Include.ALWAYS)
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
  @JsonInclude(JsonInclude.Include.NON_NULL)
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
  @JsonInclude(JsonInclude.Include.NON_NULL)
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
  @JsonInclude(JsonInclude.Include.NON_NULL)
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
  @JsonInclude(JsonInclude.Include.NON_NULL)
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
    // TODO(czhu): make Continuation token session/client-based (i.e: not reusable)
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
