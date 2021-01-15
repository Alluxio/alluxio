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

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

/**
 * Helper class for adding logs to request methods for debugging purposes.
 */
@Logged
@Provider
public class RequestLoggingFilter implements ContainerRequestFilter {
  private static final Logger LOG = LoggerFactory.getLogger(RequestLoggingFilter.class);

  @Override
  public void filter(ContainerRequestContext context) throws IOException {
    LOG.info("Method: " + context.getMethod());
    LOG.info("Path: " + context.getUriInfo().getPath());
    LOG.info("Query Parameters: " + context.getUriInfo().getQueryParameters());
    LOG.info("Path Parameters: " + context.getUriInfo().getPathParameters());
    LOG.info("Headers: " + context.getHeaders());
    LOG.info("Media Type: " + context.getMediaType());
    LOG.info("Request Message Body: " + context.getEntityStream());
  }
}
