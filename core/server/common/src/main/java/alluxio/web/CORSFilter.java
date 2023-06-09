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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * A filter for adding the Cors header to the http header.
 */
public class CORSFilter extends HttpFilter {
  @Override
  public void doFilter(HttpServletRequest req, HttpServletResponse resp, FilterChain chain)
      throws IOException, ServletException {
    if (Configuration.getBoolean(PropertyKey.WEB_CORS_ENABLED)) {
      String allowOrigins = Configuration.getString(PropertyKey.WEB_CORS_ALLOW_ORIGINS);
      String allowMethods = Configuration.getString(PropertyKey.WEB_CORS_ALLOW_METHODS);
      String allowHeaders = Configuration.getString(PropertyKey.WEB_CORS_ALLOW_HEADERS);
      String exposeHeaders = Configuration.getString(PropertyKey.WEB_CORS_EXPOSED_HEADERS);
      boolean allowCredential = Configuration.getBoolean(
          PropertyKey.WEB_CORS_ALLOW_CREDENTIAL);
      int maxAge = Configuration.getInt(PropertyKey.WEB_CORS_MAX_AGE);

      if (!StringUtils.equals(allowOrigins, "*")) {
        resp.addHeader("Vary", "Origin");
      }

      resp.setHeader("Access-Control-Allow-Origin", allowOrigins);
      resp.setHeader("Access-Control-Allow-Headers", allowHeaders);
      resp.setHeader("Access-Control-Allow-Methods", allowMethods);
      resp.setHeader("Access-Control-Max-Age", String.valueOf(maxAge));
      resp.setHeader("Access-Control-Expose-Headers", exposeHeaders);
      if (allowCredential) {
        resp.setHeader("Access-Control-Allow-Credentials", "true");
      }
    }
    chain.doFilter(req, resp);
  }
}
