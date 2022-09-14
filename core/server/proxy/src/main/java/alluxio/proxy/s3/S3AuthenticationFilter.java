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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;

/**
 * Authentication filter for S3 API.
 * It will convert the content of the Authorization Header field to the user name.
 */
@PreMatching
@Provider
public class S3AuthenticationFilter implements ContainerRequestFilter {
  private static final Logger LOG = LoggerFactory.getLogger(S3AuthenticationFilter.class);

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    if (!requestContext.getUriInfo().getPath().startsWith(S3RestServiceHandler.SERVICE_PREFIX)) {
      return;
    }
    try {
      String authorization = requestContext.getHeaderString("Authorization");
      String user = S3RestUtils.getUser(authorization, requestContext);
      // replace the authorization header value to user
      requestContext.getHeaders().replace("Authorization", Collections.singletonList(user));
    } catch (Exception e) {
      LOG.warn("exception happened in Authentication:", e);
      requestContext.abortWith(S3ErrorResponse.createErrorResponse(e, "Authorization"));
    }
  }
}
