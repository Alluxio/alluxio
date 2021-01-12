package alluxio.proxy.s3.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

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
