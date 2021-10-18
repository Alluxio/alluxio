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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

/**
 * Helper class for adding logs to request methods for debugging purposes.
 */
@Logged
@Provider
public class RequestLoggingFilter implements ContainerResponseFilter {
  private static final Logger LOG = LoggerFactory.getLogger(RequestLoggingFilter.class);
  private static final String SERVER_LOG_DELIMITER = " - ";

  @Context
  private HttpServletRequest mRequest;

  @Override
  public void filter(ContainerRequestContext req, ContainerResponseContext resp)
          throws IOException {
    StringBuilder builder = new StringBuilder();
    builder.append(mRequest.getRemoteHost()).append(SERVER_LOG_DELIMITER)
            .append(req.getMethod()).append(SERVER_LOG_DELIMITER)
            .append(req.getUriInfo().getPath()).append(SERVER_LOG_DELIMITER)
            .append(req.getLength()).append(SERVER_LOG_DELIMITER)
            .append(resp.getStatus()).append(SERVER_LOG_DELIMITER)
            .append(req.getUriInfo().getQueryParameters()).append(SERVER_LOG_DELIMITER)
            .append(req.getHeaders());
    LOG.info(builder.toString());
  }
}
