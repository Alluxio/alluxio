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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet that provides data for the header navigation bar.
 */
@ThreadSafe
public final class WebInterfaceHeaderServlet extends HttpServlet {
  private static final long serialVersionUID = -2466055439220042703L;

  /**
   * Creates a new instance of {@link WebInterfaceHeaderServlet}.
   */
  public WebInterfaceHeaderServlet() {}

  /**
   * Populate the header with information about master. So we can return to
   * the master from any page.
   *
   * @param request the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   * @throws ServletException if the target resource throws this exception
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    int masterWebPort = Configuration.getInt(PropertyKey.MASTER_WEB_PORT);
    String masterHostName;
    if (!Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
      masterHostName = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC);
    } else {
      masterHostName = NetworkAddressUtils
          .getLeaderAddressFromZK(Configuration.get(PropertyKey.ZOOKEEPER_LEADER_PATH))
          .getHostName();
    }
    request.setAttribute("masterHost", masterHostName);
    request.setAttribute("masterPort", masterWebPort);
    getServletContext().getRequestDispatcher("/header.jsp").include(request, response);
  }
}
