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

package alluxio.proxy.s3.tag;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

/**
 * Tagging object according to the specs in
 * https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html#API_GetBucketTagging_ResponseSyntax.
 */
@JacksonXmlRootElement(localName = "Tagging")
public class Tagging {
  private TagSet mTagSet;

  /**
   * Default constructor used for XML deserialization.
   */
  public Tagging() {}

  /**
   * Constructs a Tagging object.
   * @param tagMap map of tag keys to tag values
   */
  public Tagging(Map<String, String> tagMap) {
    final ArrayList<String> keys = new ArrayList<>(tagMap.keySet());
    Collections.sort(keys);

    final ArrayList<Tag> tags = new ArrayList<>();

    for (String key : keys) {
      tags.add(new Tag(key, tagMap.get(key)));
    }

    mTagSet = new TagSet(tags);
  }

  /**
   * @return the tagset
   */
  @JacksonXmlProperty(localName = "TagSet")
  public TagSet getTagSet() {
    return mTagSet;
  }

  /**
   * @param tagSet the tagset
   * @return the updated object
   */
  public Tagging setTagSet(TagSet tagSet) {
    mTagSet = tagSet;
    return this;
  }
}
