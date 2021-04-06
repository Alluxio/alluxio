package alluxio.web;

import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.UnavailableException;
import alluxio.master.FaultTolerantAlluxioMasterProcess;
import alluxio.master.MasterInquireClient;
import alluxio.master.PrimarySelector;
import alluxio.security.user.ServerUserState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URLEncoder;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class RedirectionFilter implements Filter {
  private static final Logger LOG = LoggerFactory.getLogger(RedirectionFilter.class);

  private static final Pattern[] NO_REDIRECTION_PATH_PATTERNS = new Pattern[]{
          Pattern.compile(".*\\.js"),
          Pattern.compile(".*\\.css"),
          Pattern.compile(".*\\.png"),
          Pattern.compile(".*\\.ico")
  };

  private PrimarySelector mPrimarySelector;

  RedirectionFilter(PrimarySelector primarySelector) {
    mPrimarySelector = primarySelector;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    // Nothing to initialize.
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
          throws IOException, ServletException {
    // We only have logic that handles HttpServletRequest
    if (!(request instanceof HttpServletRequest)) {
      LOG.warn("Request {} is not HttpServletRequest", request.getClass());
      return;
    }
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    String path = httpRequest.getRequestURI();

    // For resources and the page itself, do not redirect
    for (Pattern pattern : NO_REDIRECTION_PATH_PATTERNS) {
      Matcher matcher = pattern.matcher(path);
      if (matcher.find()) {
        // Forward the request.
        chain.doFilter(request, response);
        return;
      }
    }

    if (isPrimary()) {
      // The master is the primary, continue
      chain.doFilter(request, response);
      return;
    }

    // If the master is not primary, figure out the current primary address and redirect
    // The request is from the UI
    HttpServletResponse httpResponse = (HttpServletResponse) response;

    // Generate the primary address
    try {
      // TODO(jiacheng): how to write the redirect URL
      httpResponse.sendRedirect(String.format("%s?redirect=%s", getPrimaryHostname().toString(),
              URLEncoder.encode(path, "utf-8")));
      return;
    } catch (UnavailableException e) {
      // TODO(jiacheng): better msg here?
      httpResponse.sendError(500, "The primary is unavailable.");
    }
  }

  @Override
  public void destroy() {
    // Nothing to destroy.
  }


  private boolean isPrimary() {
    return mPrimarySelector.getState() == PrimarySelector.State.PRIMARY;
  }

  private InetSocketAddress getPrimaryHostname() throws UnavailableException {
    MasterInquireClient client = MasterInquireClient.Factory.create(ServerConfiguration.global(), ServerUserState.global());
    return client.getPrimaryRpcAddress();
  }
}