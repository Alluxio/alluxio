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

import alluxio.client.file.URIStatus;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of ListMultipartUploadsResult according to
 * https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html.
 */
// TODO(czhu): Support more fields (MaxUploads, NextUploadIdMarker, NextUploadIdMarker, etc.)
// - use options similar to ListBucketOptions
@JacksonXmlRootElement(localName = "ListMultipartUploadsResult")
@JsonPropertyOrder({ "Bucket", "Upload" })
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ListMultipartUploadsResult {
  private static final Logger LOG = LoggerFactory.getLogger(ListMultipartUploadsResult.class);

  private String mBucket;
  private List<ListMultipartUploadsResult.Upload> mUploads;

  /**
   * @param bucket the bucket to list multipart uploads for
   * @param children a list of {@link URIStatus}, representing the multipart upload metadata files
   * @return a {@link ListMultipartUploadsResult}
   */
  public static ListMultipartUploadsResult buildFromStatuses(String bucket,
                                                             List<URIStatus> children) {
    List<ListMultipartUploadsResult.Upload> uploads = children.stream()
        .filter(status -> {
          if (status.getXAttr() == null
              || !status.getXAttr().containsKey(S3Constants.UPLOADS_BUCKET_XATTR_KEY)
              || !status.getXAttr().containsKey(S3Constants.UPLOADS_OBJECT_XATTR_KEY)) {
            LOG.warn("Skipping multipart upload {}, metadata is missing", status.getName());
            return false;
          }
          return bucket.equals(new String(status.getXAttr().get(
              S3Constants.UPLOADS_BUCKET_XATTR_KEY), S3Constants.XATTR_STR_CHARSET));
        })
        .map(status -> new Upload(
            new String(status.getXAttr().get(S3Constants.UPLOADS_OBJECT_XATTR_KEY),
                S3Constants.XATTR_STR_CHARSET),
            status.getName(),
            S3RestUtils.toS3Date(status.getLastModificationTimeMs())
        ))
        .collect(Collectors.toList());
    return new ListMultipartUploadsResult(bucket, uploads);
  }

  /**
   * Creates a {@link ListMultipartUploadsResult}.
   * Empty constructor for deserialization
   */
  public ListMultipartUploadsResult() {}

  /**
   * Creates a {@link ListMultipartUploadsResult}.
   *
   * @param bucket the S3 bucket name
   * @param uploads the list of Upload objects
   */
  private ListMultipartUploadsResult(String bucket,
                                    List<ListMultipartUploadsResult.Upload> uploads) {
    mBucket = bucket;
    mUploads = uploads;
  }

  /**
   * @return the bucket name
   */
  @JacksonXmlProperty(localName = "Bucket")
  public String getBucket() {
    return mBucket;
  }

  /**
   * @return the list of uploads
   */
  @JacksonXmlProperty(localName = "Upload")
  @JacksonXmlElementWrapper(useWrapping = false)
  public List<ListMultipartUploadsResult.Upload> getUploads() {
    return mUploads;
  }

  /**
   * @param bucket the bucket
   */
  @JacksonXmlProperty(localName = "Bucket")
  public void setBucket(String bucket) {
    mBucket = bucket;
  }

  /**
   * @param uploads the list of Upload Objects
   */
  @JacksonXmlProperty(localName = "Upload")
  public void setUploads(List<ListMultipartUploadsResult.Upload> uploads) {
    mUploads = uploads;
  }

  /**
   * The Upload POJO.
   */
  @JacksonXmlRootElement(localName = "Upload")
  @JsonPropertyOrder({ "Key", "UploadId", "Initiated" })
  public static class Upload {
    String mKey;
    String mUploadId;
    String mInitiated;

    // Empty constructor for deserialization
    Upload() {}

    Upload(String key, String uploadId, String initiated) {
      mKey = key;
      mUploadId = uploadId;
      mInitiated = initiated;
    }

    /**
     * @return the object key
     */
    @JacksonXmlProperty(localName = "Key")
    public String getKey() {
      return mKey;
    }

    /**
     * @return the timestamp for when the multipart upload was initiated
     */
    @JacksonXmlProperty(localName = "Initiated")
    public String getInitiated() {
      return mInitiated;
    }

    /**
     * @return the upload ID
     */
    @JacksonXmlProperty(localName = "UploadId")
    public String getUploadId() {
      return mUploadId;
    }

    /**
     * @param key the S3 object key
     */
    @JacksonXmlProperty(localName = "Key")
    public void setKey(String key) {
      mKey = key;
    }

    /**
     * @param initiated the creation timestamp of the multipart upload
     */
    @JacksonXmlProperty(localName = "Initiated")
    public void setInitiated(String initiated) {
      mInitiated = initiated;
    }

    /**
     * @param uploadId the upload ID string
     */
    @JacksonXmlProperty(localName = "UploadId")
    public void setUploadId(String uploadId) {
      mUploadId = uploadId;
    }
  }
}
