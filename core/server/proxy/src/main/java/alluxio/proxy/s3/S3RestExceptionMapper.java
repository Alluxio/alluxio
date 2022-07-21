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

import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.PermissionDeniedException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * An exception mapper for REST PROXY to convert inner exception to HTTP Response.
 */
@Provider
public class S3RestExceptionMapper implements ExceptionMapper<Throwable> {
  private static final Logger LOG = LoggerFactory.getLogger(S3RestExceptionMapper.class);

  /**
   * convert the Exception to the HTTP Response for jersey.
   *
   * @param e the exception to map to a response
   * @return Response Http Response
   */
  @Override
  public Response toResponse(Throwable e) {
    if (e instanceof AlluxioStatusException) {
      return createErrorResponse((AlluxioStatusException) e);
    } else if (e instanceof S3Exception) {
      return S3RestUtils.createErrorResponse((S3Exception) e);
    } else if (e instanceof IOException) {
      return createErrorResponse((IOException) e);
    } else {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(e.getMessage()).build();
    }
  }

  /**
   * convert the AlluxioStatusException to the HTTP Response for jersey.
   * @param e AlluxioStatusException
   * @return response Http Response
   */
  private Response createErrorResponse(AlluxioStatusException e) {
    XmlMapper mapper = new XmlMapper();
    S3ErrorCode s3ErrorCode;
    // TODO(WYY): we need to handle more exception in the future.
    if (e instanceof NotFoundException) {
      // 404
      s3ErrorCode = S3ErrorCode.NO_SUCH_KEY;
    } else if (e instanceof InvalidArgumentException) {
      // 400
      s3ErrorCode = S3ErrorCode.INVALID_ARGUMENT;
    } else if (e instanceof PermissionDeniedException) {
      // 403
      s3ErrorCode = S3ErrorCode.ACCESS_DENIED_ERROR;
    } else if (e instanceof FailedPreconditionException) {
      // 412
      s3ErrorCode = S3ErrorCode.PRECONDITION_FAILED;
    } else {
      // 500
      s3ErrorCode = S3ErrorCode.INTERNAL_ERROR;
    }
    S3Error errorResponse = new S3Error(e.getMessage(), s3ErrorCode);
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

  private Response createErrorResponse(IOException e) {
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
    S3Error errorResponse = new S3Error(e.getMessage(), s3ErrorCode);
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
