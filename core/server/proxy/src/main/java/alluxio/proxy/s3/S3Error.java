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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Error response defined in http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html.
 * It will be encoded into an XML string to be returned as an error response for the REST call.
 */
@XmlRootElement(name = "Error")
public class S3Error {
  private final String mCode;
  private final String mMessage;
  private final String mRequestId;
  private final String mResource;

  /**
   * Creates an {@link S3Error}.
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
  @XmlElement(name = "Code")
  public String getCode() {
    return mCode;
  }

  /**
   * @return the error message
   */
  @XmlElement(name = "Message")
  public String getMessage() {
    return mMessage;
  }

  /**
   * @return the request ID
   */
  @XmlElement(name = "RequestId")
  public String getRequestId() {
    return mRequestId;
  }

  /**
   * @return the resource name
   */
  @XmlElement(name = "Resource")
  public String getResource() {
    return mResource;
  }
}

