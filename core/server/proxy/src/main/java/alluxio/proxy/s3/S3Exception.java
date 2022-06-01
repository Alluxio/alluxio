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

/**
 * An exception thrown during processing S3 REST requests.
 */
public class S3Exception extends Exception {
  private final S3ErrorCode mErrorCode;

  private String mResource;

  /**
   * Constructs a new {@link S3Exception}.
   *
   * @param errorCode the error code
   */
  public S3Exception(S3ErrorCode errorCode) {
    super(errorCode.getDescription());
    mErrorCode = errorCode;
  }

  /**
   * Constructs a new {@link S3Exception}.
   *
   * @param resource the resource name (bucket or object key)
   * @param errorCode the error code
   */
  public S3Exception(String resource, S3ErrorCode errorCode) {
    super(errorCode.getDescription());
    mResource = resource;
    mErrorCode = errorCode;
  }

  /**
   * Derives a new {@link S3Exception} from an existing exception.
   *
   * @param exception the existing exception
   * @param resource the resource name (bucket or object key)
   * @param errorCode the error code
   */
  public S3Exception(Exception exception, String resource, S3ErrorCode errorCode) {
    super(exception.getMessage(), exception);
    mResource = resource;
    mErrorCode = new S3ErrorCode(errorCode.getCode(), exception.getMessage(),
        errorCode.getStatus());
  }

  /**
   * Derives a new {@link S3Exception} from an existing exception.
   *
   * @param message the exception message
   * @param resource the resource name (bucket or object key)
   * @param errorCode the error code
   */
  public S3Exception(String message, String resource, S3ErrorCode errorCode) {
    mResource = resource;
    mErrorCode = new S3ErrorCode(errorCode.getCode(), message,
            errorCode.getStatus());
  }

  /**
   * @return the error code
   */
  public S3ErrorCode getErrorCode() {
    return mErrorCode;
  }

  /**
   * @return the resource name
   */
  public String getResource() {
    return mResource;
  }

  /**
   * @param resource the S3 resource string
   */
  public void setResource(String resource) {
    mResource = resource;
  }
}
