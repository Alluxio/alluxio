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

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Provides an abstract class to be subclassed to create an HTTP filter.
 */
public abstract class HttpFilter implements Filter {
  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void destroy() {
  }

  @Override
  public final void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain)
      throws IOException, ServletException {
    if (!(req instanceof HttpServletRequest && resp instanceof HttpServletResponse)) {
      throw new ServletException(
          String.format("Received non-HTTP request or response: req=%s, resp=%s",
              req.getClass(), resp.getClass()));
    }

    HttpServletRequest request = (HttpServletRequest) req;
    HttpServletResponse response = (HttpServletResponse) resp;

    doFilter(request, response, chain);
  }

  /**
   * Receives standard HTTP requests from the public doFilter method.
   * @param req http request
   * @param resp http response
   * @param chain filter chain
   */
  public abstract void doFilter(HttpServletRequest req, HttpServletResponse resp, FilterChain chain)
      throws IOException, ServletException;
}
