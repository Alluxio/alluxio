/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.web;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.master.file.FileSystemMaster;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet that provides data for displaying the system's configuration.
 */
@ThreadSafe
public final class WebInterfaceConfigurationServlet extends HttpServlet {
  private static final long serialVersionUID = 2134205675393443914L;
  private static final String ALLUXIO_CONF_PREFIX = "alluxio";
  private static final Set<String> ALLUXIO_CONF_EXCLUDES = new HashSet<String>(
      Arrays.asList(Constants.MASTER_WHITELIST));

  private final transient FileSystemMaster mFsMaster;
  private final transient Configuration mConfiguration;

  /**
   * Creates a new instance of {@link WebInterfaceConfigurationServlet}.
   *
   * @param fsMaster file system master
   */
  public WebInterfaceConfigurationServlet(FileSystemMaster fsMaster) {
    mFsMaster = fsMaster;
    mConfiguration = new Configuration();
  }

  /**
   * Populates attributes before redirecting to a jsp.
   *
   * @param request The {@link HttpServletRequest} object
   * @param response The {@link HttpServletResponse} object
   * @throws ServletException if the target resource throws this exception
   * @throws IOException if the target resource throws this exception
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    request.setAttribute("whitelist", mFsMaster.getWhiteList());
    request.setAttribute("configuration", getSortedProperties());

    getServletContext().getRequestDispatcher("/configuration.jsp").forward(request, response);
  }

  private SortedSet<Pair<String, String>> getSortedProperties() {
    TreeSet<Pair<String, String>> rtn = new TreeSet<Pair<String, String>>();
    for (Map.Entry<Object, Object> entry : mConfiguration.getInternalProperties().entrySet()) {
      String key = entry.getKey().toString();
      if (key.startsWith(ALLUXIO_CONF_PREFIX) && !ALLUXIO_CONF_EXCLUDES.contains(key)) {
        rtn.add(new ImmutablePair<String, String>(key, mConfiguration.get(key)));
      }
    }
    return rtn;
  }
}
