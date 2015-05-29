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

import tachyon.TachyonURI;
import tachyon.master.MasterInfo;
import tachyon.thrift.AccessControlException;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.InvalidPathException;

/**
 * Servlet that provides data for displaying which files are currently in memory.
 */
public class WebInterfaceMemoryServlet extends HttpServlet {
  private static final long serialVersionUID = 4293149962399443914L;
  private final transient MasterInfo mMasterInfo;

  public WebInterfaceMemoryServlet(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }

  /**
   * Populates attributes before redirecting to a jsp.
   * 
   * @param request The HttpServletRequest object
   * @param response The HttpServletReponse object
   * @throws ServletException
   * @throws IOException
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    request.setAttribute("masterNodeAddress", mMasterInfo.getMasterAddress().toString());
    request.setAttribute("fatalError", "");

    List<TachyonURI> inMemoryFiles = mMasterInfo.getInMemoryFiles();
    Collections.sort(inMemoryFiles);

    List<UiFileInfo> fileInfos = new ArrayList<UiFileInfo>(inMemoryFiles.size());
    for (TachyonURI file : inMemoryFiles) {
      try {
        ClientFileInfo fileInfo = mMasterInfo.getClientFileInfo(file);
        if (fileInfo != null && fileInfo.getInMemoryPercentage() == 100) {
          fileInfos.add(new UiFileInfo(fileInfo));
        }
      } catch (InvalidPathException ipe) {
        request.setAttribute("fatalError", "Error: Invalid Path " + ipe.getLocalizedMessage());
        getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
        return;
      } catch (AccessControlException ace) {
        request.setAttribute("fatalError", "Error: Access denied " + ace.getLocalizedMessage());
        getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
        return;
      }
    }
    request.setAttribute("inMemoryFileNum", Integer.valueOf(fileInfos.size()));

    // URL is "./memory", can not determine offset and limit, let javascript in jsp determine
    // and redirect to "./memory?offset=xxx&limit=xxx"
    if (request.getParameter("offset") == null && request.getParameter("limit") == null) {
      getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
      return;
    }

    try {
      int offset = Integer.parseInt(request.getParameter("offset"));
      int limit = Integer.parseInt(request.getParameter("limit"));
      List<UiFileInfo> sub = fileInfos.subList(offset, offset + limit);
      request.setAttribute("fileInfos", sub);
    } catch (NumberFormatException nfe) {
      request.setAttribute("fatalError",
          "Error: offset or limit parse error, " + nfe.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
      return;
    } catch (IndexOutOfBoundsException iobe) {
      request.setAttribute("fatalError",
          "Error: offset or offset + limit is out of bound, " + iobe.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
      return;
    } catch (IllegalArgumentException iae) {
      request.setAttribute("fatalError", iae.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
      return;
    }

    getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
  }
}
