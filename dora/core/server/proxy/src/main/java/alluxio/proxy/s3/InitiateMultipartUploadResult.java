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
 * Result returned after requests for initiating a multipart upload.
 * It is defined in http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html.
 * It will be encoded into an XML string to be returned as a response for the REST call.
 */
@JacksonXmlRootElement(localName = "InitiateMultipartUploadResult")
@JsonPropertyOrder({ "Bucket", "Key", "UploadId" })
public class InitiateMultipartUploadResult {
  /* Name of the bucket to which the multipart upload was initiated. */
  private String mBucket;
  /* Object key for which the multipart upload was initiated. */
  private String mKey;
  /* ID for the initiated multipart upload. */
  private String mUploadId;

  /**
   * Constructs an {@link InitiateMultipartUploadResult} with fields initialized to empty strings.
   */
  public InitiateMultipartUploadResult() {
    mBucket = "";
    mKey = "";
    mUploadId = "";
  }

  /**
   * Constructs an {@link InitiateMultipartUploadResult} with the specified values.
   *
   * @param bucket name of the bucket to which the multipart upload was initiated
   * @param key object key for which the multipart upload was initiated
   * @param uploadId ID for the initiated multipart upload
   */
  public InitiateMultipartUploadResult(String bucket, String key, String uploadId) {
    mBucket = bucket;
    mKey = key;
    mUploadId = uploadId;
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
   * @return the upload ID
   */
  @JacksonXmlProperty(localName = "UploadId")
  public String getUploadId() {
    return mUploadId;
  }

  /**
   * @param uploadId the upload ID to set
   */
  @JacksonXmlProperty(localName = "UploadId")
  public void setUploadId(String uploadId) {
    mUploadId = uploadId;
  }
}
