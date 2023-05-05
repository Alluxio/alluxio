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
 * Error response defined in http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html.
 * It will be encoded into an XML string to be returned as an error response for the REST call.
 */
@JacksonXmlRootElement(localName = "Error")
public class S3Error {
  private String mCode;
  private String mMessage;
  private String mRequestId;
  private String mResource;

  /**
   * Creates an {@link S3Error}.
   *
   * This is needed for {@link S3Error} to be encoded into XML, see <a>https://stackoverflow.com/
   * questions/26014184/jersey-returns-500-when-trying-to-return-an-xml-response</a> for details.
   */
  public S3Error() {
    mCode = "";
    mMessage = "";
    mRequestId = "";
    mResource = "";
  }

  /**
   * Creates a new {@link Error}.
   *
   * @param resource the resource (bucket or object key) where the error happens
   * @param code the error code
   */
  public S3Error(String resource, S3ErrorCode code) {
    mCode = code.getCode();
    mMessage = code.getDescription();
    mRequestId = "";
    mResource = resource;
  }

  /**
   * @return the error code
   */
  @JacksonXmlProperty(localName = "Code")
  public String getCode() {
    return mCode;
  }

  /**
   * @return the error message
   */
  @JacksonXmlProperty(localName = "Message")
  public String getMessage() {
    return mMessage;
  }

  /**
   * @return the request ID
   */
  @JacksonXmlProperty(localName = "RequestId")
  public String getRequestId() {
    return mRequestId;
  }

  /**
   * @return the resource name
   */
  @JacksonXmlProperty(localName = "Resource")
  public String getResource() {
    return mResource;
  }

  /**
   * @param code the error code to set
   */
  @JacksonXmlProperty(localName = "Code")
  public void setCode(String code) {
    mCode = code;
  }

  /**
   * @param message the error message to set
   */
  @JacksonXmlProperty(localName = "Message")
  public void setMessage(String message) {
    mMessage = message;
  }

  /**
   * @param requestId the request ID to set
   */
  @JacksonXmlProperty(localName = "RequestId")
  public void setRequestId(String requestId) {
    mRequestId = requestId;
  }

  /**
   * @param resource the resource name to set
   */
  @JacksonXmlProperty(localName = "Resource")
  public void setResource(String resource) {
    mResource = resource;
  }
}

