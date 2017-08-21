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

package alluxio.proxy;

import alluxio.security.LoginUser;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.SecurityUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.ws.rs.core.Response;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Utilities for handling S3 REST calls.
 */
public final class S3RestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(S3RestUtils.class);

  /**
   * Calls the given {@link S3RestUtils.RestCallable} and handles any exceptions thrown.
   *
   * @param <T> the return type of the callable
   * @param resource the resource (bucket or object) to be operated on
   * @param callable the callable to call
   * @return the response object
   */
  public static <T> Response call(String resource, S3RestUtils.RestCallable<T> callable) {
    try {
      if (SecurityUtils.isSecurityEnabled() && AuthenticatedClientUser.get() == null) {
        AuthenticatedClientUser.set(LoginUser.get().getName());
      }
    } catch (IOException e) {
      LOG.warn("Failed to set AuthenticatedClientUser in REST service handler: {}", e.getMessage());
      return createErrorResponse(new S3Exception(e, resource, ErrorCode.INTERNAL_ERROR));
    }

    try {
      return createResponse(callable.call());
    } catch (S3Exception e) {
      LOG.warn("Unexpected error invoking REST endpoint: {}", e.getErrorCode().getDescription());
      return createErrorResponse(e);
    }
  }

  /**
   * An interface representing a callable.
   *
   * @param <T> the return type of the callable
   */
  public interface RestCallable<T> {
    /**
     * The REST endpoint implementation.
     *
     * @return the return value from the callable
     */
    T call() throws S3Exception;
  }

  /**
   * Creates a response using the given object.
   *
   * @param object the object to respond with
   * @param resource the resource name (bucket or object key)
   * @return the response
   */
  private static Response createResponse(Object object) {
    if (object instanceof Void) {
      return Response.ok().build();
    }
    return Response.ok(object).build();
  }

  /**
   * Creates an error response using the given exception.
   *
   * @param e the exception to be converted into {@link Error} and encoded into XML
   * @return the response
   */
  private static Response createErrorResponse(S3Exception e) {
    Error errorResponse = new Error(e.getResource(), e.getErrorCode());
    return Response.status(e.getErrorCode().getStatus()).entity(errorResponse).build();
  }

  /**
   * Error code names used in {@link ErrorCode}.
   */
  public static class ErrorCodeName {
    public static final String BUCKET_ALREADY_EXISTS = "BucketAlreadyExists";
    public static final String INTERNAL_ERROR = "InternalError";
    public static final String INVALID_BUCKET_NAME = "InvalidBucketName";

    /**
     * Default constructor.
     */
    public ErrorCodeName() {}
  }

  /**
   * Error codes defined in http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html,
   * some are customized.
   */
  public static class ErrorCode {
    //
    // Official error codes.
    //
    public static final ErrorCode BUCKET_ALREADY_EXISTS = new ErrorCode(
        ErrorCodeName.BUCKET_ALREADY_EXISTS,
        "The requested bucket name already exists",
        Response.Status.CONFLICT);
    public static final ErrorCode INVALID_BUCKET_NAME = new ErrorCode(
        ErrorCodeName.INVALID_BUCKET_NAME,
        "The specified bucket name is invalid",
        Response.Status.BAD_REQUEST);
    public static final ErrorCode INTERNAL_ERROR = new ErrorCode(ErrorCodeName.INTERNAL_ERROR,
        "We encountered an internal error. Please try again.",
        Response.Status.INTERNAL_SERVER_ERROR);

    //
    // Customized error codes.
    //
    public static final ErrorCode INVALID_NESTED_BUCKET_NAME = new ErrorCode(
        ErrorCodeName.BUCKET_ALREADY_EXISTS,
        "The specified bucket is not a directory directly under a mount point",
        Response.Status.BAD_REQUEST);

    private final String mCode;
    private final String mDescription;
    private final Response.Status mStatus;

    /**
     * Constructs a new {@link ErrorCode}.
     *
     * @param code the error code
     * @param description the description
     * @param status the response status
     */
    public ErrorCode(String code, String description, Response.Status status) {
      mCode = code;
      mDescription = description;
      mStatus = status;
    }

    /**
     * @return the error code
     */
    public String getCode() {
      return mCode;
    }

    /**
     * @return the description
     */
    public String getDescription() {
      return mDescription;
    }

    /**
     * @return the HTTP status
     */
    public Response.Status getStatus() {
      return mStatus;
    }
  }

  /**
   * An exception thrown during processing S3 REST requests.
   */
  public static class S3Exception extends Exception {
    private final String mResource;
    private final ErrorCode mErrorCode;

    /**
     * Constructs a new {@link S3Exception}.
     *
     * @param resource the resource name (bucket or object key)
     * @param errorCode the error code
     */
    public S3Exception(String resource, ErrorCode errorCode) {
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
    public S3Exception(Exception exception, String resource, ErrorCode errorCode) {
      mResource = resource;
      mErrorCode = new ErrorCode(errorCode.getCode(), exception.getMessage(),
          errorCode.getStatus());
    }

    /**
     * @return the error code
     */
    public ErrorCode getErrorCode() {
      return mErrorCode;
    }

    /**
     * @return the resource name
     */
    public String getResource() {
      return mResource;
    }
  }

  /**
   * Error response defined in http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html.
   * It will be encoded into an XML string to be returned as an error response for the REST call.
   */
  @XmlRootElement(name = "Error")
  public static class Error {
    private final String mCode;
    private final String mMessage;
    private final String mRequestId;
    private final String mResource;

    /**
     * Creates an {@link Error}.
     * This is needed for {@link Error} to be encoded into XML, see <a>https://stackoverflow.com/
     * questions/26014184/jersey-returns-500-when-trying-to-return-an-xml-response</a> for details.
     */
    public Error() {
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
    public Error(String resource, ErrorCode code) {
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

  private S3RestUtils() {} // prevent instantiation
}
