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

/**
 * The Tag object according to https://docs.aws.amazon.com/AmazonS3/latest/API/API_Tag.html.
 */
public class Tag {
  private String mKey;
  private String mValue;

  /**
   * Default Constructor used for XML deserialization.
   */
  public Tag() {}

  Tag(String key, String value) {
    mKey = key;
    mValue = value;
  }

  /**
   * @return the key
   */
  @JacksonXmlProperty(localName = "Key")
  public String getKey() {
    return mKey;
  }

  /**
   * @return the value
   */
  @JacksonXmlProperty(localName = "Value")
  public String getValue() {
    return mValue;
  }

  /**
   * @param key the key
   * @return the updated object
   */
  public Tag setKey(String key) {
    mKey = key;
    return this;
  }

  /**
   * @param value the value
   * @return the updated object
   */
  public Tag setValue(String value) {
    mValue = value;
    return this;
  }
}
