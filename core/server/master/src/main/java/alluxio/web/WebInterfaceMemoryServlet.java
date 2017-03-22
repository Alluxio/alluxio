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
import alluxio.master.AlluxioMasterService;
import alluxio.security.LoginUser;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.SecurityUtils;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Servlet that provides data for displaying which files are currently in memory.
 */
@ThreadSafe
public final class WebInterfaceMemoryServlet extends HttpServlet {
  private static final long serialVersionUID = 4293149962399443914L;
  private final transient AlluxioMasterService mMaster;

  /**
   * Creates a new instance of {@link WebInterfaceMemoryServlet}.
   *
   * @param master Alluxio master
   */
  public WebInterfaceMemoryServlet(AlluxioMasterService master) {
    mMaster = Preconditions.checkNotNull(master);
  }

  /**
   * Populates attributes before redirecting to a jsp.
   *
   * @param request the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   * @throws ServletException if the target resource throws this exception
   * @throws IOException if the target resource throws this exception
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    if (SecurityUtils.isSecurityEnabled() && AuthenticatedClientUser.get() == null) {
      AuthenticatedClientUser.set(LoginUser.get().getName());
    }
    request.setAttribute("masterNodeAddress", mMaster.getRpcAddress().toString());
    request.setAttribute("fatalError", "");
    request.setAttribute("showPermissions",
        Configuration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED));
    getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
  }
}
