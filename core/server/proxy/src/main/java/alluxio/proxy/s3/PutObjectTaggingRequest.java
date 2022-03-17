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

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.ArrayList;
import java.util.List;

/**
 * Parsed version of a request to PutObjectTagging.
 * See https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html
 */
@JacksonXmlRootElement(localName = "Tagging")
public class PutObjectTaggingRequest {

  @JacksonXmlProperty(localName = "TagSet")
  private TagSet mTagSet;

  /**
   * Default constructor for jackson.
   */
  public PutObjectTaggingRequest() {
    mTagSet = new TagSet();
  }

  /**
   * Create a new {@link PutObjectTaggingRequest}.
   *
   * @param tagSet the user metadata tags
   */
  public PutObjectTaggingRequest(TagSet tagSet) {
    mTagSet = tagSet;
  }

  /**
   * Sets the tag set.
   *
   * @param tagSet set of user metadata tags
   */
  @JacksonXmlProperty(localName = "TagSet")
  public void setTagSet(TagSet tagSet) {
    mTagSet = tagSet;
  }

  /**
   * @return the tag set
   */
  @JacksonXmlProperty(localName = "TagSet")
  public TagSet getTagSet() {
    return mTagSet;
  }

  /**
   * Inner POJO representing the user metadata tag set in the S3 API.
   */
  @JacksonXmlRootElement(localName = "TagSet")
  static class TagSet {
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
  }

  /**
   * Inner POJO representing a user metadata tag in the S3 API.
   */
  @JacksonXmlRootElement(localName = "Tag")
  static class TagObject {
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
      // TODO(czhu): perform tag validation
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
  }
}
