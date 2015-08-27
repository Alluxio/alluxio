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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import tachyon.master.MasterInfo;
import tachyon.thrift.ClientDependencyInfo;
import tachyon.thrift.DependencyDoesNotExistException;
import tachyon.thrift.FileDoesNotExistException;

public class WebInterfaceDependencyServlet extends HttpServlet {
  private static final long serialVersionUID = 2071462168900313417L;
  private final transient MasterInfo mMasterInfo;

  public WebInterfaceDependencyServlet(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    request.setAttribute("masterNodeAddress", mMasterInfo.getMasterAddress().toString());
    request.setAttribute("filePath", request.getParameter("filePath"));
    request.setAttribute("error", "");
    int dependencyId = Integer.parseInt(request.getParameter("id"));
    List<String> parentFileNames = new ArrayList<String>();
    List<String> childrenFileNames = new ArrayList<String>();
    try {
      ClientDependencyInfo dependencyInfo = mMasterInfo.getClientDependencyInfo(dependencyId);
      for (int pId : dependencyInfo.parents) {
        parentFileNames.add(mMasterInfo.getPath(pId).toString());
      }
      for (int cId : dependencyInfo.children) {
        childrenFileNames.add(mMasterInfo.getPath(cId).toString());
      }
    } catch (DependencyDoesNotExistException ddnee) {
      request.setAttribute("error", ddnee.getMessage());
    } catch (FileDoesNotExistException fdne) {
      request.setAttribute("error", fdne.getMessage());
    }
    Collections.sort(parentFileNames);
    Collections.sort(childrenFileNames);
    request.setAttribute("parentFileNames", parentFileNames);
    request.setAttribute("childrenFileNames", childrenFileNames);
    getServletContext().getRequestDispatcher("/dependency.jsp").forward(request, response);
  }

  @Override
  public void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    return;
  }
}
