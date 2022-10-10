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

package alluxio.proxy.s3.logging;

import org.glassfish.jersey.server.ContainerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;

/**
 * Helper class for adding logs to request methods for debugging purposes.
 */
@Logged
@PreMatching
@Provider
public class RequestLoggingFilter implements ContainerRequestFilter {
  private static final Logger LOG = LoggerFactory.getLogger(RequestLoggingFilter.class);

  @Override
  public void filter(ContainerRequestContext context) throws IOException {
    if (LOG.isInfoEnabled()) {
      // Build log message capturing the request details
      StringBuilder sb = new StringBuilder();
      sb.append("Alluxio S3 API received ");
      sb.append(context.getMethod());
      sb.append(" request: URI=");
      sb.append(((ContainerRequest) context).getRequestUri().toString());
      sb.append(" User=");
      sb.append(context.getHeaderString("Authorization"));
      sb.append(" Media Type=");
      sb.append(context.getMediaType());
      sb.append(" Query Parameters=");
      sb.append(context.getUriInfo().getQueryParameters());
      sb.append(" Path Parameters=");
      sb.append(context.getUriInfo().getPathParameters());
      if (LOG.isDebugEnabled()) {
        if (context.getHeaders() != null) {
          sb.append(" Headers=");
          Map<String, String> headerMap = new HashMap<>();
          for (String headerName : context.getHeaders().keySet()) {
            headerMap.put(headerName, context.getHeaderString(headerName));
          }
          sb.append(headerMap);
        }
        LOG.debug(sb.toString());
      } else {
        LOG.info(sb.toString());
      }
    }
  }
}
