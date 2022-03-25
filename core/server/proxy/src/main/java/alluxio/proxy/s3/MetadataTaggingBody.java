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

import alluxio.Constants;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Parsed version of a request to PutObjectTagging.
 * See https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html
 */
@JacksonXmlRootElement(localName = "Tagging")
public class MetadataTaggingBody implements Serializable {
  public static final long serialVersionUID = 1L;

  @JacksonXmlProperty(localName = "TagSet")
  private TagSet mTagSet;

  /**
   * Default constructor for jackson.
   */
  public MetadataTaggingBody() {
    mTagSet = new TagSet();
  }

  /**
   * Create a new {@link MetadataTaggingBody}.
   *
   * @param tagSet the user metadata tags
   */
  public MetadataTaggingBody(TagSet tagSet) {
    setTagSet(tagSet); // use setter method for tag validation
  }

  /**
   * Sets the tag set.
   *
   * @param tagSet set of user metadata tags
   */
  @JacksonXmlProperty(localName = "TagSet")
  public void setTagSet(TagSet tagSet) {
    mTagSet = tagSet;
    validateTags();
  }

  /**
   * @return the tag set
   */
  @JacksonXmlProperty(localName = "TagSet")
  public TagSet getTagSet() {
    return mTagSet;
  }

  @Override
  public String toString() {
    return mTagSet.toString();
  }

  /**
   * Validates S3 User tag restrictions.
   */
  void validateTags() {
    List<TagObject> tags = mTagSet.getTags();
    int totalBytes = 0;
    if (tags.size() == 0) { return; }
    try {
      if (tags.size() > 10) {
        throw new S3Exception(new S3ErrorCode(
            S3ErrorCode.BAD_REQUEST.getCode(),
            "User-defined metadata tags cannot be greater than 10",
            S3ErrorCode.BAD_REQUEST.getStatus()));
      }
      Set<String> tagKeys = new HashSet<>();
      for (TagObject tag : tags) {
        // Tag key validation
        if (tag.mKey.length() > 128) {
          throw new S3Exception(new S3ErrorCode(
              S3ErrorCode.INVALID_TAG.getCode(),
              String.format("Tag key exceeds maximum length (128): %s", tag.mKey),
              S3ErrorCode.INVALID_TAG.getStatus()));
        }
        if (tagKeys.contains(tag.mKey)) {
          throw new S3Exception(new S3ErrorCode(
              S3ErrorCode.INVALID_TAG.getCode(),
              "Tags cannot contain duplicate keys",
              S3ErrorCode.INVALID_TAG.getStatus()));
        }
        tagKeys.add(tag.mKey);
        // Tag value validation
        if (tag.mValue.length() > 256) {
          throw new S3Exception(new S3ErrorCode(
              S3ErrorCode.INVALID_TAG.getCode(),
              String.format("Tag value exceeds maximum length (256): %s=%s", tag.mKey, tag.mValue),
              S3ErrorCode.INVALID_TAG.getStatus()));
        }
        // Header user-metadata size limit validation (<= 2 KB)
        // - https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html
        totalBytes += tag.getNumBytes();
        if (totalBytes > 2 * Constants.KB) {
          throw new S3Exception(S3ErrorCode.METADATA_TOO_LARGE);
        }
      }
    } catch (S3Exception e) {
      // IllegalArgumentException will be consumed by IOException from the
      // jersey library when parsing the XML into this object
      // - the underlying S3Exception will be the throwable cause for the IOException
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Inner POJO representing the user metadata tag set in the S3 API.
   */
  @JacksonXmlRootElement(localName = "TagSet")
  static class TagSet implements Serializable {
    @JacksonXmlProperty(localName = "Tag")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<TagObject> mTags;

    /**
     * Default constructor for jackson.
     */
    public TagSet() { mTags = new ArrayList<TagObject>(); }

    /**
     * @param tags the tags
     */
    public TagSet(List<TagObject> tags) {
      mTags = tags;
    }

    /**
     * @return the tags
     */
    @JacksonXmlProperty(localName = "Tag")
    public List<TagObject> getTags() {
      return mTags;
    }

    /**
     * Sets the key to the object.
     *
     * @param tags the tags
     */
    @JacksonXmlProperty(localName = "Tag")
    public void setTags(List<TagObject> tags) {
      mTags = tags;
    }

    @Override
    public String toString() {
      return mTags.toString();
    }
  }

  /**
   * Inner POJO representing a user metadata tag in the S3 API.
   */
  @JacksonXmlRootElement(localName = "Tag")
  static class TagObject implements Serializable {
    private String mKey;
    private String mValue;

    /**
     * Default constructor for jackson.
     */
    public TagObject() { }

    /**
     * @param key the key indicating the tag name
     * @param value the key indicating the tag value
     */
    public TagObject(String key, String value) {
      mKey = key;
      mValue = value;
    }

    /**
     * @return the key of the object
     */
    @JacksonXmlProperty(localName = "Key")
    public String getKey() {
      return mKey;
    }

    /**
     * @return the key of the object
     */
    @JacksonXmlProperty(localName = "Value")
    public String getValue() {
      return mValue;
    }

    /**
     * Sets the key to the object.
     *
     * @param key the key
     */
    @JacksonXmlProperty(localName = "Key")
    public void setKey(String key) {
      mKey = key;
    }

    /**
     * Sets the value to the object.
     *
     * @param value the value
     */
    @JacksonXmlProperty(localName = "Value")
    public void setValue(String value) {
      mValue = value;
    }

    @Override
    public String toString() {
      return mKey + " = " + mValue;
    }

    /**
     * Convenience method for calculating metadata size limits.
     * @return the sum of the number of bytes in the UTF-8 encoding of the tag key and value
     */
    int getNumBytes() {
      return mKey.getBytes(S3Constants.TAGGING_CHARSET).length
          + mValue.getBytes(S3Constants.TAGGING_CHARSET).length;
    }
  }
}
