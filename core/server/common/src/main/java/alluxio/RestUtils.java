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

package alluxio;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.Status;
import alluxio.security.LoginUser;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.SecurityUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.ws.rs.core.Response;

/**
 * Utilities for handling REST calls.
 */
public final class RestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(RestUtils.class);

  /**
   * Calls the given {@link RestUtils.RestCallable} and handles any exceptions thrown.
   *
   * @param <T> the return type of the callable
   * @param callable the callable to call
   * @return the response object
   */
  public static <T> Response call(RestUtils.RestCallable<T> callable) {
    try {
      if (SecurityUtils.isSecurityEnabled() && AuthenticatedClientUser.get() == null) {
        AuthenticatedClientUser.set(LoginUser.get().getName());
      }
    } catch (IOException e) {
      LOG.warn("Failed to set AuthenticatedClientUser in REST service handler: {}", e.getMessage());
      return createErrorResponse(e);
    }

    try {
      return createResponse(callable.call());
    } catch (Exception e) {
      LOG.warn("Unexpected error invoking rest endpoint: {}", e.getMessage());
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
    T call() throws Exception;
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
    if (object instanceof String) {
      // Need to explicitly encode the string as JSON because Jackson will not do it automatically.
      ObjectMapper mapper = new ObjectMapper();
      try {
        return Response.ok(mapper.writeValueAsString(object)).build();
      } catch (JsonProcessingException e) {
        return createErrorResponse(e);
      }
    }
    return Response.ok(object).build();
  }

  /**
   * Error response when {@link RestCallable#call()} throws an exception.
   * It will be encoded into a Json string to be returned as an error response for the REST call.
   */
  public static class ErrorResponse {
    private final Status mStatus;
    private final String mMessage;

    /**
     * Creates a new {@link ErrorResponse}.
     *
     * @param status the RPC call result's {@link Status}
     * @param message the error message
     */
    public ErrorResponse(Status status, String message) {
      mStatus = status;
      mMessage = message;
    }

    /**
     * @return the status
     */
    public Status getStatus() {
      return mStatus;
    }

    /**
     * @return the error message
     */
    public String getMessage() {
      return mMessage;
    }
  }

  /**
   * Creates an error response using the given exception.
   *
   * @param e the exception to be converted into {@link ErrorResponse} and encoded into json
   * @return the response
   */
  private static Response createErrorResponse(Exception e) {
    AlluxioStatusException se = AlluxioStatusException.fromThrowable(e);
    ErrorResponse response = new ErrorResponse(se.getStatus(), se.getMessage());
    return Response.serverError().entity(response).build();
  }

  private RestUtils() {} // prevent instantiation
}
