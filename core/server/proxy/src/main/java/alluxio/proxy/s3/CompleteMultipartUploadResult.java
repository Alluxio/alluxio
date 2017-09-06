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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

/**
 * Result returned after requests for completing a multipart upload.
 * It is defined in http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html.
 * It will be encoded into an XML string to be returned as a response for the REST call.
 */
@JacksonXmlRootElement(localName = "CompleteMultipartUploadResult")
@JsonPropertyOrder({ "Location", "Bucket", "Key", "ETag" })
public class CompleteMultipartUploadResult {
  /* The URI that identifies the newly created object. */
  private String mLocation;
  /* Name of the bucket. */
  private String mBucket;
  /* Object key. */
  private String mKey;
  /* Entity tag of the object. */
  private String mETag;

  /**
   * Constructs an {@link CompleteMultipartUploadResult} with fields initialized to empty strings.
   */
  public CompleteMultipartUploadResult() {
    mLocation = "";
    mBucket = "";
    mKey = "";
    mETag = "";
  }

  /**
   * Constructs an {@link CompleteMultipartUploadResult} with the specified values.
   *
   * @param location the URI that identifies the newly created object
   * @param bucket name of the bucket
   * @param key object key
   * @param etag entity tag of the newly created object, the etag should not be surrounded by quotes
   */
  public CompleteMultipartUploadResult(String location, String bucket, String key, String etag) {
    mLocation = location;
    mBucket = bucket;
    mKey = key;
    mETag = S3RestUtils.quoteETag(etag);
  }

  /**
   * @return the location
   */
  @JacksonXmlProperty(localName = "Location")
  public String getLocation() {
    return mLocation;
  }

  /**
   * @param location the location to set
   */
  @JacksonXmlProperty(localName = "Location")
  public void setLocation(String location) {
    mLocation = location;
  }

  /**
   * @return the bucket name
   */
  @JacksonXmlProperty(localName = "Bucket")
  public String getBucket() {
    return mBucket;
  }

  /**
   * @param bucket the bucket name to set
   */
  @JacksonXmlProperty(localName = "Bucket")
  public void setBucket(String bucket) {
    mBucket = bucket;
  }

  /**
   * @return the object key
   */
  @JacksonXmlProperty(localName = "Key")
  public String getKey() {
    return mKey;
  }

  /**
   * @param key the object key to set
   */
  @JacksonXmlProperty(localName = "Key")
  public void setKey(String key) {
    mKey = key;
  }

  /**
   * @return the entity tag surrounded by quotes
   */
  @JacksonXmlProperty(localName = "ETag")
  public String getETag() {
    return mETag;
  }

  /**
   * @param etag the entity tag to be set, should not be surrounded by quotes
   */
  @JacksonXmlProperty(localName = "ETag")
  public void setETag(String etag) {
    mETag = S3RestUtils.quoteETag(etag);
  }
}
