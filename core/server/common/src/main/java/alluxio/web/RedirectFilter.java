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

package alluxio.web;

import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterInquireClient;
import alluxio.master.PrimarySelector;
import alluxio.security.user.ServerUserState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * The {@link RedirectFilter} enables redirecting request to primary master node when current node
 * is not primary.
 */
public class RedirectFilter implements Filter {
  private static final Logger LOG = LoggerFactory.getLogger(RedirectFilter.class);

  private static final Pattern[] NO_REDIRECTION_PATH_PATTERNS = new Pattern[]{
      Pattern.compile("/metrics/json"),
      Pattern.compile("/metrics/prometheus")
  };

  /** primary selector. */
  private final PrimarySelector mPrimarySelector;

  /** primary selector. */
  private final Supplier<Integer> mGetWebPortSupplier;

  /**
   * Initializes the redirect filter.
   *
   * @param primarySelector primary selector
   */
  public RedirectFilter(PrimarySelector primarySelector) {
    this(primarySelector, null);
  }

  /**
   * Initializes the redirect filter.
   *
   * @param primarySelector primary selector
   * @param getWebPortSupplier the supplier to get primary master web port
   */
  public RedirectFilter(PrimarySelector primarySelector, Supplier<Integer> getWebPortSupplier) {
    mPrimarySelector = primarySelector;
    mGetWebPortSupplier = getWebPortSupplier;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
          throws IOException, ServletException {
    HttpServletResponse httpResponse = (HttpServletResponse) response;
    if (isPrimary()) {
      // The master is the primary, continue
      chain.doFilter(request, response);
      return;
    }
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    String path = httpRequest.getRequestURI();
    // Do not redirect for some path
    for (Pattern pattern : NO_REDIRECTION_PATH_PATTERNS) {
      Matcher matcher = pattern.matcher(path);
      if (matcher.find()) {
        chain.doFilter(request, response);
        return;
      }
    }

    try {
      // Generate the primary address
      InetSocketAddress primaryAddress = getPrimaryAddress();
      if (mGetWebPortSupplier != null) {
        int webPort = mGetWebPortSupplier.get();
        // Forward the request.
        String leaderWebAddress = "http://" + primaryAddress.getHostString() + ":"
            + webPort + httpRequest.getRequestURI();
        LOG.debug("redirect request to {}", leaderWebAddress);
        httpResponse.sendRedirect(leaderWebAddress);
        return;
      }
      httpResponse.sendError(500,
          "The current node is not the primary node, and the primary node is located in "
          + primaryAddress);
    } catch (UnavailableException e) {
      httpResponse.sendError(500, "The primary is unavailable.");
    }
  }

  @Override
  public void destroy() {
  }

  private boolean isPrimary() {
    return mPrimarySelector.getState() == PrimarySelector.State.PRIMARY;
  }

  private InetSocketAddress getPrimaryAddress() throws UnavailableException {
    MasterInquireClient client = MasterInquireClient.Factory.create(
        ServerConfiguration.global(), ServerUserState.global());

    return client.getPrimaryRpcAddress();
  }
}
