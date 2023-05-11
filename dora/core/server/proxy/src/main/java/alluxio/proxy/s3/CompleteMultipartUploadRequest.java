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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Implementation of CompleteMultipartUploadRequest according to
 * https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html.
 */
// TODO(czhu): Support more fields (MaxUploads, NextUploadIdMarker, NextUploadIdMarker, etc.)
// - use options similar to ListBucketOptions
@JacksonXmlRootElement(localName = "CompleteMultipartUploadRequest")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class CompleteMultipartUploadRequest {
  private static final Logger LOG = LoggerFactory.getLogger(CompleteMultipartUploadRequest.class);

  private List<CompleteMultipartUploadRequest.Part> mParts;

  /**
   * Creates a {@link CompleteMultipartUploadRequest}.
   * Empty constructor for deserialization
   */
  public CompleteMultipartUploadRequest() {}

  /**
   * Creates a {@link CompleteMultipartUploadRequest}.
   *
   * @param parts the list of Part objects
   */
  public CompleteMultipartUploadRequest(List<Part> parts) {
    this(parts, false);
  }

  /**
   * Creates a {@link CompleteMultipartUploadRequest}.
   * This is used exclusively for unit test purposes.
   *
   * @param parts the list of Part objects
   * @param ignoreValidation flag to skip Part validation
   */
  public CompleteMultipartUploadRequest(List<Part> parts, boolean ignoreValidation) {
    if (ignoreValidation) {
      mParts = parts;
    } else {
      setParts(parts);
    }
  }

  /**
   * @return the list of uploads
   */
  @JacksonXmlProperty(localName = "Part")
  @JacksonXmlElementWrapper(useWrapping = false)
  public List<Part> getParts() {
    return mParts;
  }

  /**
   * @param parts the list of Part Objects
   */
  @JacksonXmlProperty(localName = "Part")
  public void setParts(List<Part> parts) {
    mParts = parts;
    validateParts();
  }

  private void validateParts() {
    if (mParts.size() <= 1) { return; }
    try {
      int prevPartNum = mParts.get(0).getPartNumber();
      for (Part part : mParts.subList(1, mParts.size())) {
        if (prevPartNum + 1 != part.getPartNumber()) {
          throw new S3Exception(S3ErrorCode.INVALID_PART_ORDER);
        }
        prevPartNum = part.getPartNumber();
      }
    } catch (S3Exception e) {
      // IllegalArgumentException will be consumed by IOException from the
      // jersey library when parsing the XML into this object
      // - the underlying S3Exception will be the throwable cause for the IOException
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * The Part POJO.
   */
  @JacksonXmlRootElement(localName = "Part")
  @JsonPropertyOrder({ "ETag", "PartNumber" })
  public static class Part {
    String mETag;
    int mPartNumber;

    /**
     * Creates a {@link CompleteMultipartUploadRequest.Part}.
     * Empty constructor for deserialization
     */
    public Part() {}

    /**
     * Creates a {@link CompleteMultipartUploadRequest.Part}.
     * @param eTag the ETag
     * @param partNumber the part number
     */
    public Part(String eTag, int partNumber) {
      mETag = eTag;
      mPartNumber = partNumber;
    }

    /**
     * @return the ETag
     */
    @JacksonXmlProperty(localName = "ETag")
    public String getETag() {
      return mETag;
    }

    /**
     * @return the part number
     */
    @JacksonXmlProperty(localName = "PartNumber")
    public int getPartNumber() {
      return mPartNumber;
    }

    /**
     * @param eTag the Etag
     */
    @JacksonXmlProperty(localName = "ETag")
    public void setKey(String eTag) {
      mETag = eTag;
    }

    /**
     * @param partNumber the part number
     */
    @JacksonXmlProperty(localName = "PartNumber")
    public void setKey(int partNumber) {
      mPartNumber = partNumber;
    }
  }
}
