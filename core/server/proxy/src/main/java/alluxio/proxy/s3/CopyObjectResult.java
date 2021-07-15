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

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

/**
 * Result of an S3 Copy.
 */
@JacksonXmlRootElement(localName = "CopyObjectResult")
public class CopyObjectResult {

  private final String mETag;
  private final String mLastModified;

  /**
   * Create a new {@link CopyObjectResult}.
   *
   * @param etag etag included in the result
   * @param lastModifiedEpoch epoch time in ms which is converted to a timestamp string in the
   *                          output
   */
  public CopyObjectResult(String etag, long lastModifiedEpoch) {
    mETag = etag;
    mLastModified = S3RestUtils.toS3Date(lastModifiedEpoch);
  }

  /**
   * @return the ETag
   */
  @JacksonXmlProperty(localName = "ETag")
  public String getEtag() {
    return mETag;
  }

  /**
   * @return the last modified timestamp
   */
  @JacksonXmlProperty(localName = "LastModified")
  public String getLastModified() {
    return mLastModified;
  }
}
