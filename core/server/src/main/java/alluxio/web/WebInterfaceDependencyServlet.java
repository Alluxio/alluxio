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

import alluxio.master.AlluxioMaster;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet for dependencies, such as parent and children file names in the web interface.
 */
@ThreadSafe
public final class WebInterfaceDependencyServlet extends HttpServlet {
  private static final long serialVersionUID = 2071462168900313417L;
  private final transient AlluxioMaster mMaster;

  /**
   * Creates a new instance of {@link WebInterfaceDependencyServlet}.
   *
   * @param master Alluxio master
   */
  public WebInterfaceDependencyServlet(AlluxioMaster master) {
    mMaster = Preconditions.checkNotNull(master);
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    request.setAttribute("masterNodeAddress", mMaster.getMasterAddress().toString());
    request.setAttribute("filePath", request.getParameter("filePath"));
    request.setAttribute("error", "");
    List<String> parentFileNames = new ArrayList<String>();
    List<String> childrenFileNames = new ArrayList<String>();
    Collections.sort(parentFileNames);
    Collections.sort(childrenFileNames);
    request.setAttribute("parentFileNames", parentFileNames);
    request.setAttribute("childrenFileNames", childrenFileNames);
  }

  @Override
  public void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    return;
  }
}
