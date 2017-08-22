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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.ws.rs.core.Response;

/**
 * Utilities for handling S3 REST calls.
 */
public final class S3RestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(S3RestUtils.class);

  public static final String DELETE_IN_ALLUXIO_ONLY = "ALLUXIO_ONLY";

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
    S3Error errorResponse = new S3Error(e.getResource(), e.getErrorCode());
    return Response.status(e.getErrorCode().getStatus()).entity(errorResponse).build();
  }

  private S3RestUtils() {} // prevent instantiation
}
