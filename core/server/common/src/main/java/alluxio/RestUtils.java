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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.user.ServerUserState;
import alluxio.util.SecurityUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

/**
 * Utilities for handling REST calls.
 */
public final class RestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(RestUtils.class);

  /**
   * Calls the given {@link RestUtils.RestCallable} and handles any exceptions thrown.
   *
   * @param <T>  the return type of the callable
   * @param callable the callable to call
   * @param alluxioConf Alluxio configuration
   * @param headers the headers
   * @return the response object
   */
  public static <T> Response call(RestUtils.RestCallable<T> callable,
      AlluxioConfiguration alluxioConf, @Nullable Map<String, Object> headers) {
    try {
      // TODO(cc): reconsider how to enable authentication
      if (SecurityUtils.isSecurityEnabled(alluxioConf)
          && AuthenticatedClientUser.get(alluxioConf) == null) {
        AuthenticatedClientUser.set(ServerUserState.global().getUser().getName());
      }
    } catch (IOException e) {
      LOG.warn("Failed to set AuthenticatedClientUser in REST service handler: {}", e.toString());
      return createErrorResponse(e, alluxioConf);
    }

    try {
      return createResponse(callable.call(), alluxioConf, headers);
    } catch (Exception e) {
      LOG.warn("Unexpected error invoking rest endpoint: {}", e.toString());
      return createErrorResponse(e, alluxioConf);
    }
  }

  /**
   * Call response.
   *
   * @param <T>  the type parameter
   * @param callable the callable
   * @param alluxioConf the alluxio conf
   * @return the response
   */
  public static <T> Response call(RestUtils.RestCallable<T> callable,
      AlluxioConfiguration alluxioConf) {
    return call(callable, alluxioConf, null);
  }

  /**
   * An interface representing a callable.
   *
   * @param <T>  the return type of the callable
   */
  public interface RestCallable<T> {
    /**
     * The REST endpoint implementation.
     *
     * @return the return value from the callable
     * @throws Exception the exception
     */
    T call() throws Exception;
  }

  /**
   * Creates a response using the given object.
   *
   * @param object the object to respond with
   * @return the response
   */
  private static Response createResponse(Object object, AlluxioConfiguration alluxioConf,
      @Nullable Map<String, Object> headers) {
    if (object instanceof Void) {
      return Response.ok().build();
    }
    if (object instanceof String) {
      // Need to explicitly encode the string as JSON because Jackson will not do it automatically.
      ObjectMapper mapper = new ObjectMapper();
      try {
        return Response.ok(mapper.writeValueAsString(object)).build();
      } catch (JsonProcessingException e) {
        return createErrorResponse(e, alluxioConf);
      }
    }

    Response.ResponseBuilder rb = Response.ok(object);
    if (headers != null) {
      headers.forEach(rb::header);
    }

    if (alluxioConf.getBoolean(PropertyKey.WEB_CORS_ENABLED)) {
      return makeCORS(rb).build();
    }

    return rb.build();
  }

  /**
   * Error response when {@link RestCallable#call()} throws an exception.
   * It will be encoded into a Json string to be returned as an error response for the REST call.
   */
  public static class ErrorResponse {
    private final Status.Code mStatusCode;
    private final String mMessage;

    /**
     * Creates a new {@link ErrorResponse}.
     *
     * @param statusCode the RPC call result's {@link Status.Code}
     * @param message the error message
     */
    public ErrorResponse(Status.Code statusCode, String message) {
      mStatusCode = statusCode;
      mMessage = message;
    }

    /**
     * Gets status.
     *
     * @return the status
     */
    public Status.Code getStatusCode() {
      return mStatusCode;
    }

    /**
     * Gets message.
     *
     * @return the message
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
  private static Response createErrorResponse(Exception e, AlluxioConfiguration alluxioConf) {
    AlluxioStatusException se = AlluxioStatusException.fromThrowable(e);
    ErrorResponse response = new ErrorResponse(se.getStatus().getCode(), se.getMessage());

    Response.ResponseBuilder rb = Response.serverError().entity(response);
    if (alluxioConf.getBoolean(PropertyKey.WEB_CORS_ENABLED)) {
      return makeCORS(rb).build();
    }

    return rb.build();
  }

  /**
   * Makes the responseBuilder CORS compatible.
   *
   * @param responseBuilder the response builder
   * @param returnMethod the modified response builder
   * @return response builder
   */
  public static Response.ResponseBuilder makeCORS(Response.ResponseBuilder responseBuilder,
      String returnMethod) {
    // TODO(william): Make origin, methods, and headers configurable.
    Response.ResponseBuilder rb = responseBuilder.header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");

    if (!"".equals(returnMethod)) {
      rb.header("Access-Control-Allow-Headers", returnMethod);
    }

    return rb;
  }

  /**
   *  Makes the responseBuilder CORS compatible, assumes default methods.
   *
   * @param responseBuilder the modified response builder
   * @return response builder
   */
  public static Response.ResponseBuilder makeCORS(Response.ResponseBuilder responseBuilder) {
    return makeCORS(responseBuilder, "");
  }

  private RestUtils() {
  } // prevent instantiation
}
