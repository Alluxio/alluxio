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

import alluxio.security.LoginUser;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.SecurityUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.ws.rs.core.Response;

/**
 * Utilities for handling S3 REST calls.
 */
public final class S3RestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(S3RestUtils.class);

  public static final String S3_STANDARD_STORAGE_CLASS = "STANDARD";
  public static final String S3_EMPTY_ETAG = "";
  public static final String S3_DATE_FORMAT_REGEXP = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  public static final int S3_DEFAULT_MAX_KEYS = 1000;

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
      // TODO(cc): reconsider how to enable authentication
      if (SecurityUtils.isSecurityEnabled() && AuthenticatedClientUser.get() == null) {
        AuthenticatedClientUser.set(LoginUser.get().getName());
      }
    } catch (IOException e) {
      LOG.warn("Failed to set AuthenticatedClientUser in REST service handler: {}", e.getMessage());
      return createErrorResponse(new S3Exception(e, resource, S3ErrorCode.INTERNAL_ERROR));
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
   * @return the response
   */
  private static Response createResponse(Object object) {
    if (object == null) {
      return Response.ok().build();
    }

    if (object instanceof Response.Status) {
      Response.Status s = (Response.Status) object;
      switch (s) {
        case OK:
          return Response.ok().build();
        case ACCEPTED:
          return Response.accepted().build();
        case NO_CONTENT:
          return Response.noContent().build();
        default:
          return createErrorResponse(
              new S3Exception("Response status is invalid", S3ErrorCode.INTERNAL_ERROR));
      }
    }

    // Need to explicitly encode the string as XML because Jackson will not do it automatically.
    XmlMapper mapper = new XmlMapper();
    try {
      return Response.ok(mapper.writeValueAsString(object)).build();
    } catch (JsonProcessingException e) {
      return createErrorResponse(new S3Exception(e.getMessage(), S3ErrorCode.INTERNAL_ERROR));
    }
  }

  /**
   * Creates an error response using the given exception.
   *
   * @param e the exception to be converted into {@link Error} and encoded into XML
   * @return the response
   */
  private static Response createErrorResponse(S3Exception e) {
    S3Error errorResponse = new S3Error(e.getResource(), e.getErrorCode());
    return Response.status(e.getErrorCode().getStatus()).entity(errorResponse).build();
  }

  private S3RestUtils() {} // prevent instantiation
}
