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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.xml.bind.annotation.XmlTransient;

/**
 * Parsed version of the S3 tagging XML.
 * Used by the following S3 API actions:
 * - GetBucketTagging, GetObjectTagging, PutBucketTagging, PutObjectTagging
 */
@JacksonXmlRootElement(localName = "Tagging")
@JsonIgnoreProperties(ignoreUnknown = true, value = {"tagMap"})
public class TaggingData {
  private static final XmlMapper MAPPER = new XmlMapper();

  @XmlTransient
  private final Map<String, String> mTagMap;

  /**
   * Deserialize the XML (bytes).
   * @param bytes byte array of the Java-serialized object
   * @return a deserialized {@link TaggingData} object
   */
  public static TaggingData deserialize(byte[] bytes)
      throws IOException {
    return MAPPER.readerFor(TaggingData.class).readValue(bytes);
  }

  /**
   * Serializes the object as an XML (bytes).
   * @param tagData the {@link TaggingData} object to serialize
   * @return the {@link ByteString} serialization of this object
   */
  public static ByteString serialize(TaggingData tagData)
      throws JsonProcessingException {
    return ByteString.copyFrom(MAPPER.writeValueAsBytes(tagData));
  }

  @JacksonXmlProperty(localName = "TagSet")
  private TagSet mTagSet;

  /**
   * Default constructor for jackson.
   */
  public TaggingData() {
    this(new TagSet());
  }

  /**
   * Create a new {@link TaggingData}.
   *
   * @param tagSet the user metadata tags
   */
  public TaggingData(@Nullable TagSet tagSet) {
    mTagMap = new HashMap<>();
    setTagSet(tagSet); // use setter method for tag validation
  }

  /**
   * Sets the tag set.
   *
   * @param tagSet set of user metadata tags
   */
  @JacksonXmlProperty(localName = "TagSet")
  private void setTagSet(@Nullable TagSet tagSet) {
    if (tagSet == null) { // forced to accept null parameter for unmarshalling XML strings
      tagSet = new TagSet();
    }
    mTagSet = tagSet;
    validateTags();
    repopulateTagMap();
  }

  /**
   * @return the tag set
   */
  @JacksonXmlProperty(localName = "TagSet")
  private TagSet getTagSet() {
    return mTagSet;
  }

  @Override
  public String toString() {
    return mTagSet.toString();
  }

  /**
   * Returns the Map object containing the contents of
   * the TagSet. Useful for parsing or iterating over tag data.
   * @return a Map object containing the tags
   */
  @XmlTransient
  public Map<String, String> getTagMap() {
    return mTagMap;
  }

  /**
   * Adds the provided key-value pair as a new tag
   * Overwrites any duplicate tags.
   * @param key the tag's key
   * @param value the tag's value
   */
  private void addTag(String key, String value) {
    if (!mTagMap.containsKey(key)) {
      mTagSet.mTags.add(new TagObject(key, value));
      mTagMap.put(key, value);
    } // else, we have to update the existing tag
    for (TagObject tag : mTagSet.mTags) {
      if (tag.mKey.equals(key)) {
        tag.mValue = value;
        break;
      }
    }
    mTagMap.put(key, value);
  }

  /**
   * Merges the contents of the provided tag map with
   * the existing tags. Overwrites any duplicate tags.
   * @param tagMap a collection of Tags represented by key-value pairs
   * @return a reference to this object
   */
  @XmlTransient
  public TaggingData addTags(Map<String, String> tagMap) throws IllegalArgumentException {
    for (Map.Entry<String, String> tag : tagMap.entrySet()) {
      addTag(tag.getKey(), tag.getValue());
    }
    validateTags();
    return this;
  }

  /**
   * Fills the tag map with the contents of the tag set.
   */
  private void repopulateTagMap() {
    mTagMap.clear();
    for (TagObject tag : mTagSet.mTags) {
      mTagMap.put(tag.mKey, tag.mValue);
    }
  }

  /**
   * Validates S3 User tag restrictions.
   */
  private void validateTags() throws IllegalArgumentException {
    List<TagObject> tags = mTagSet.getTags();
    if (tags.size() == 0) { return; }
    try {
      if (tags.size() > 10) {
        throw new S3Exception(new S3ErrorCode(
            S3ErrorCode.INVALID_TAG.getCode(),
            "User-defined metadata tags cannot be greater than 10",
            S3ErrorCode.INVALID_TAG.getStatus()));
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
  private static class TagSet {
    @JacksonXmlProperty(localName = "Tag")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<TagObject> mTags;

    /**
     * Default constructor for jackson.
     */
    public TagSet() { this(new ArrayList<TagObject>()); }

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
    private List<TagObject> getTags() {
      return mTags;
    }

    /**
     * Sets the key to the object.
     *
     * @param tags the tags
     */
    @JacksonXmlProperty(localName = "Tag")
    private void setTags(List<TagObject> tags) {
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
  private static class TagObject {
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
    private String getKey() {
      return mKey;
    }

    /**
     * @return the key of the object
     */
    @JacksonXmlProperty(localName = "Value")
    private String getValue() {
      return mValue;
    }

    /**
     * Sets the key to the object.
     *
     * @param key the key
     */
    @JacksonXmlProperty(localName = "Key")
    private void setKey(String key) {
      mKey = key;
    }

    /**
     * Sets the value to the object.
     *
     * @param value the value
     */
    @JacksonXmlProperty(localName = "Value")
    private void setValue(String value) {
      mValue = value;
    }

    @Override
    public String toString() {
      return mKey + " = " + mValue;
    }
  }
}
