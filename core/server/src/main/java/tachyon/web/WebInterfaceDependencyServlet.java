/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.web;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Preconditions;

import tachyon.master.TachyonMaster;

/**
 * Servlet for dependencies, such as parent and children file names in the web interface.
 */
@ThreadSafe
public final class WebInterfaceDependencyServlet extends HttpServlet {
  private static final long serialVersionUID = 2071462168900313417L;
  private final transient TachyonMaster mMaster;

  /**
   * Creates a new instance of {@link WebInterfaceDependencyServlet}.
   *
   * @param master Tachyon master
   */
  public WebInterfaceDependencyServlet(TachyonMaster master) {
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
