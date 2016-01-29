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

import tachyon.FileInfo;
import tachyon.TachyonURI;
import tachyon.exception.AccessControlException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.master.MasterContext;
import tachyon.master.TachyonMaster;
import tachyon.security.LoginUser;
import tachyon.security.authentication.PlainSaslServer;
import tachyon.util.SecurityUtils;

/**
 * Servlet that provides data for displaying which files are currently in memory.
 */
@ThreadSafe
public final class WebInterfaceMemoryServlet extends HttpServlet {
  private static final long serialVersionUID = 4293149962399443914L;
  private final transient TachyonMaster mMaster;

  /**
   * Creates a new instance of {@link WebInterfaceMemoryServlet}.
   *
   * @param master Tachyon master
   */
  public WebInterfaceMemoryServlet(TachyonMaster master) {
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
    if (SecurityUtils.isSecurityEnabled(MasterContext.getConf())
        && PlainSaslServer.AuthorizedClientUser.get(MasterContext.getConf()) == null) {
      PlainSaslServer.AuthorizedClientUser.set(LoginUser.get(MasterContext.getConf()).getName());
    }
    request.setAttribute("masterNodeAddress", mMaster.getMasterAddress().toString());
    request.setAttribute("fatalError", "");

    List<TachyonURI> inMemoryFiles = mMaster.getFileSystemMaster().getInMemoryFiles();
    Collections.sort(inMemoryFiles);

    List<UIFileInfo> fileInfos = new ArrayList<UIFileInfo>(inMemoryFiles.size());
    for (TachyonURI file : inMemoryFiles) {
      try {
        long fileId = mMaster.getFileSystemMaster().getFileId(file);
        FileInfo fileInfo = mMaster.getFileSystemMaster().getFileInfo(fileId);
        if (fileInfo != null && fileInfo.getInMemoryPercentage() == 100) {
          fileInfos.add(new UIFileInfo(fileInfo));
        }
      } catch (FileDoesNotExistException e) {
        request.setAttribute("fatalError",
            "Error: File does not exist " + e.getLocalizedMessage());
        getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
        return;
      } catch (AccessControlException e) {
        request.setAttribute("permissionError",
            "Error: File " + file + " cannot be accessed " + e.getMessage());
        getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
        return;
      }
    }
    request.setAttribute("inMemoryFileNum", fileInfos.size());

    // URL is "./memory", can not determine offset and limit, let javascript in jsp determine
    // and redirect to "./memory?offset=xxx&limit=xxx"
    if (request.getParameter("offset") == null && request.getParameter("limit") == null) {
      getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
      return;
    }

    try {
      int offset = Integer.parseInt(request.getParameter("offset"));
      int limit = Integer.parseInt(request.getParameter("limit"));
      List<UIFileInfo> sub = fileInfos.subList(offset, offset + limit);
      request.setAttribute("fileInfos", sub);
    } catch (NumberFormatException e) {
      request.setAttribute("fatalError",
          "Error: offset or limit parse error, " + e.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
      return;
    } catch (IndexOutOfBoundsException e) {
      request.setAttribute("fatalError",
          "Error: offset or offset + limit is out of bound, " + e.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
      return;
    } catch (IllegalArgumentException e) {
      request.setAttribute("fatalError", e.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
      return;
    }

    getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
  }
}
