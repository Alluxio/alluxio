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

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

/**
 * An exception mapper for REST PROXY to convert inner exception to HTTP Response.
 */
@Provider
public class S3RestExceptionMapper implements ExceptionMapper<Throwable> {
  /**
   * convert the Exception to the HTTP Response for jersey.
   *
   * @param e the exception to map to a response
   * @return Response Http Response
   */
  @Override
  public Response toResponse(Throwable e) {
    if (e instanceof AlluxioStatusException) {
      return S3RestUtils.createErrorResponse(null, (AlluxioStatusException) e);
    } else if (e instanceof AlluxioRuntimeException) {
      return S3RestUtils.createErrorResponse(null, (AlluxioRuntimeException) e);
    } else if (e instanceof S3Exception) {
      return S3RestUtils.createErrorResponse((S3Exception) e);
    } else if (e instanceof IOException) {
      return S3RestUtils.createErrorResponse(null, (IOException) e);
    } else {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(e.getMessage()).build();
    }
  }
}
