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

package alluxio.s3;

import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.exception.status.AlluxioStatusException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
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
    if (e instanceof NotFoundRuntimeException) {
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
   * Creates an error response using the given exception.
   *
   * @param e a {@link Throwable} object
   * @param resource an S3 resource key
   * @return response Http {@link Response}
   */
  public static HttpResponse createNettyErrorResponse(Throwable e, String resource) {
    if (e instanceof AlluxioStatusException) {
      return createNettyErrorResponse((AlluxioStatusException) e, resource);
    } else if (e instanceof AlluxioRuntimeException) {
      return createNettyErrorResponse((AlluxioRuntimeException) e, resource);
    } else if (e instanceof S3Exception) {
      return createNettyErrorResponse((S3Exception) e, resource);
    } else if (e instanceof IOException) {
      return createNettyErrorResponse((IOException) e, resource);
    } else {
      ByteBuf contentBuffer =
          Unpooled.copiedBuffer(e.getMessage(), CharsetUtil.UTF_8);
      FullHttpResponse resp = new DefaultFullHttpResponse(NettyRestUtils.HTTP_VERSION,
          HttpResponseStatus.INTERNAL_SERVER_ERROR, contentBuffer);
      resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
      resp.headers().set(HttpHeaderNames.CONTENT_LENGTH, resp.content().readableBytes());
      return resp;
    }
  }

  /**
   * Creates an error response using the given exception.
   *
   * @param e the exception to be converted into {@link Error} and encoded into XML
   * @param resource resource
   * @return the response
   */
  public static HttpResponse createNettyErrorResponse(S3Exception e, String resource) {
    return convertS3ErrorCodeToResponse(e.getErrorCode(), e.getMessage(), resource);
  }

  /**
   * convert the AlluxioStatusException to the HTTP Response.
   * @param e AlluxioStatusException
   * @param resource resource
   * @return response Http Response
   */
  private static HttpResponse createNettyErrorResponse(AlluxioStatusException e, String resource) {
    S3ErrorCode s3ErrorCode;
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
    return convertS3ErrorCodeToResponse(s3ErrorCode, e.getMessage(), resource);
  }

  /**
   * convert the IOException to the HTTP Response.
   * @param e IOException
   * @param resource resource
   * @return response Http Response
   */
  private static HttpResponse createNettyErrorResponse(IOException e, String resource) {
    S3ErrorCode s3ErrorCode;
    // TODO(WYY): we need to handle more exception in the future.
    if (e instanceof FileNotFoundException) {
      // 404
      s3ErrorCode = S3ErrorCode.NO_SUCH_KEY;
    } else {
      // 500
      s3ErrorCode = S3ErrorCode.INTERNAL_ERROR;
    }
    return convertS3ErrorCodeToResponse(s3ErrorCode, e.getMessage(), resource);
  }

  /**
   * convert the IOException to the HTTP Response.
   * @param e AlluxioRuntimeException
   * @param resource resource
   * @return response Http Response
   */
  private static HttpResponse createNettyErrorResponse(AlluxioRuntimeException e, String resource) {
    S3ErrorCode s3ErrorCode;
    if (e instanceof NotFoundRuntimeException) {
      // 404
      s3ErrorCode = S3ErrorCode.NO_SUCH_KEY;
    } else {
      // 500
      s3ErrorCode = S3ErrorCode.INTERNAL_ERROR;
    }
    return convertS3ErrorCodeToResponse(s3ErrorCode, e.getMessage(), resource);
  }

  private static FullHttpResponse convertS3ErrorCodeToResponse(S3ErrorCode s3ErrorCode,
                                                               String message, String resource) {
    XmlMapper mapper = new XmlMapper();
    S3Error errorResponse = new S3Error(resource, s3ErrorCode);
    errorResponse.setMessage(message);
    try {
      ByteBuf contentBuffer =
          Unpooled.copiedBuffer(mapper.writeValueAsString(errorResponse), CharsetUtil.UTF_8);
      FullHttpResponse response = new DefaultFullHttpResponse(NettyRestUtils.HTTP_VERSION,
          HttpResponseStatus.valueOf(s3ErrorCode.getStatus().getStatusCode()), contentBuffer);
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_XML);
      response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
      return response;
    } catch (JsonProcessingException e2) {
      ByteBuf contentBuffer =
          Unpooled.copiedBuffer("Failed to encode XML: " + e2.getMessage(), CharsetUtil.UTF_8);
      FullHttpResponse response = new DefaultFullHttpResponse(NettyRestUtils.HTTP_VERSION,
          HttpResponseStatus.INTERNAL_SERVER_ERROR, contentBuffer);
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
      response.headers().set(HttpHeaderNames.CONTENT_LENGTH, contentBuffer.readableBytes());
      return response;
    } finally {
      LOG.warn("mapper convert exception {} to {}.", message, s3ErrorCode.getStatus().toString());
    }
  }
}
