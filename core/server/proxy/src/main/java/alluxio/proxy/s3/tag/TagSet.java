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

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import java.util.List;

/**
 * TagSet implementation according to the specs in
 * https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html#API_GetBucketTagging_ResponseSyntax.
 */
public class TagSet {
  private List<Tag> mTags;

  /**
   * Default Constructor used for XML deserialization.
   */
  public TagSet() {}

  TagSet(List<Tag> tags) {
    mTags = tags;
  }

  /**
   * @return the list of tags
   */
  @JacksonXmlProperty(localName = "Tag")
  @JacksonXmlElementWrapper(useWrapping = false)
  public List<Tag> getTags() {
    return mTags;
  }

  /**
   * @param tags the list of tags
   * @return the updated object
   */
  public TagSet setTags(List<Tag> tags) {
    mTags = tags;
    return this;
  }
}
