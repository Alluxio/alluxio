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

import alluxio.exception.AlluxioRuntimeException;
import alluxio.exception.status.AlluxioStatusException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import javax.ws.rs.core.Response;

/**
 * Utilities for creating HTTP Responses for the S3 API.
 */
public class S3ErrorResponse {
  private static final Logger LOG = LoggerFactory.getLogger(S3ErrorResponse.class);

  /**
   * Creates an error response using the given exception.
   *
   * @param e a {@link Throwable} object
   * @param resource an S3 resource key
   * @return response Http {@link Response}
   */
  public static Response createErrorResponse(Throwable e, String resource) {
    if (e instanceof AlluxioStatusException) {
      return createErrorResponse((AlluxioStatusException) e, resource);
    } else if (e instanceof AlluxioRuntimeException) {
      return createErrorResponse((AlluxioRuntimeException) e, resource);
    } else if (e instanceof S3Exception) {
      return createErrorResponse((S3Exception) e, resource);
    } else if (e instanceof IOException) {
      return createErrorResponse((IOException) e, resource);
    } else {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(e.getMessage()).build();
    }
  }

  /**
   * Creates an error response using the given exception.
   *
   * @param e the exception to be converted into {@link Error} and encoded into XML
   * @param resource resource
   * @return the response
   */
  private static Response createErrorResponse(S3Exception e, String resource) {
    S3Error errorResponse = new S3Error(resource, e.getErrorCode());
    // Need to explicitly encode the string as XML because Jackson will not do it automatically.
    XmlMapper mapper = new XmlMapper();
    try {
      return Response.status(e.getErrorCode().getStatus())
          .entity(mapper.writeValueAsString(errorResponse)).build();
    } catch (JsonProcessingException e2) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Failed to encode XML: " + e2.getMessage()).build();
    }
  }

  /**
   * convert the AlluxioStatusException to the HTTP Response.
   * @param e AlluxioStatusException
   * @param resource resource
   * @return response Http Response
   */
  private static Response createErrorResponse(AlluxioStatusException e, String resource) {
    XmlMapper mapper = new XmlMapper();
    S3ErrorCode s3ErrorCode;
    // TODO(WYY): we need to handle more exception in the future.
    if (e instanceof alluxio.exception.status.NotFoundException) {
      // 404
      s3ErrorCode = S3ErrorCode.NO_SUCH_KEY;
    } else if (e instanceof alluxio.exception.status.InvalidArgumentException) {
      // 400
      s3ErrorCode = S3ErrorCode.INVALID_ARGUMENT;
    } else if (e instanceof alluxio.exception.status.PermissionDeniedException) {
      // 403
      s3ErrorCode = S3ErrorCode.ACCESS_DENIED_ERROR;
    } else if (e instanceof alluxio.exception.status.FailedPreconditionException) {
      // 412
      s3ErrorCode = S3ErrorCode.PRECONDITION_FAILED;
    } else {
      // 500
      s3ErrorCode = S3ErrorCode.INTERNAL_ERROR;
    }
    S3Error errorResponse = new S3Error(resource, s3ErrorCode);
    errorResponse.setMessage(e.getMessage());
    try {
      return Response.status(s3ErrorCode.getStatus())
          .entity(mapper.writeValueAsString(errorResponse)).build();
    } catch (JsonProcessingException e2) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Failed to encode XML: " + e2.getMessage()).build();
    } finally {
      LOG.warn("mapper convert exception {} to {}.", e.getClass().getName(),
          s3ErrorCode.getStatus().toString());
    }
  }

  /**
   * convert the IOException to the HTTP Response.
   * @param e IOException
   * @param resource resource
   * @return response Http Response
   */
  private static Response createErrorResponse(IOException e, String resource) {
    XmlMapper mapper = new XmlMapper();
    S3ErrorCode s3ErrorCode;
    // TODO(WYY): we need to handle more exception in the future.
    if (e instanceof FileNotFoundException) {
      // 404
      s3ErrorCode = S3ErrorCode.NO_SUCH_KEY;
    } else {
      // 500
      s3ErrorCode = S3ErrorCode.INTERNAL_ERROR;
    }
    S3Error errorResponse = new S3Error(resource, s3ErrorCode);
    errorResponse.setMessage(e.getMessage());
    try {
      return Response.status(s3ErrorCode.getStatus())
          .entity(mapper.writeValueAsString(errorResponse)).build();
    } catch (JsonProcessingException e2) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Failed to encode XML: " + e2.getMessage()).build();
    } finally {
      LOG.warn("mapper convert exception {} to {}.", e.getClass().getName(),
          s3ErrorCode.getStatus().toString());
    }
  }

  /**
   * convert the IOException to the HTTP Response.
   * @param e AlluxioRuntimeException
   * @param resource resource
   * @return response Http Response
   */
  private static Response createErrorResponse(AlluxioRuntimeException e, String resource) {
    XmlMapper mapper = new XmlMapper();
    S3ErrorCode s3ErrorCode;
    // TODO(WYY): we need to handle more exception in the future.
    // 500
    s3ErrorCode = S3ErrorCode.INTERNAL_ERROR;
    S3Error errorResponse = new S3Error(resource, s3ErrorCode);
    errorResponse.setMessage(e.getMessage());
    try {
      return Response.status(s3ErrorCode.getStatus())
          .entity(mapper.writeValueAsString(errorResponse)).build();
    } catch (JsonProcessingException e2) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Failed to encode XML: " + e2.getMessage()).build();
    } finally {
      LOG.warn("mapper convert exception {} to {}.", e.getClass().getName(),
          s3ErrorCode.getStatus().toString());
    }
  }
}
