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

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.master.MasterProcess;
import alluxio.master.file.FileSystemMaster;
import alluxio.security.LoginUser;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.SecurityUtils;
import alluxio.wire.FileInfo;

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
 * Servlet that provides data for displaying which files are currently in memory.
 */
@ThreadSafe
public final class WebInterfaceMemoryServlet extends HttpServlet {
  private static final long serialVersionUID = 4293149962399443914L;
  private final transient MasterProcess mMasterProcess;

  /**
   * Creates a new instance of {@link WebInterfaceMemoryServlet}.
   *
   * @param masterProcess Alluxio master process
   */
  public WebInterfaceMemoryServlet(MasterProcess masterProcess) {
    mMasterProcess = Preconditions.checkNotNull(masterProcess, "masterProcess");
  }

  /**
   * Populates attributes before redirecting to a jsp.
   *
   * @param request the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   * @throws ServletException if the target resource throws this exception
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    if (!Configuration.getBoolean(PropertyKey.WEB_FILE_INFO_ENABLED)) {
      return;
    }
    if (SecurityUtils.isSecurityEnabled() && AuthenticatedClientUser.get() == null) {
      AuthenticatedClientUser.set(LoginUser.get().getName());
    }
    request.setAttribute("masterNodeAddress", mMasterProcess.getRpcAddress().toString());
    request.setAttribute("fatalError", "");
    request.setAttribute("showPermissions",
        Configuration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED));

    FileSystemMaster fileSystemMaster = mMasterProcess.getMaster(FileSystemMaster.class);

    List<AlluxioURI> inAlluxioFiles = fileSystemMaster.getInAlluxioFiles();
    Collections.sort(inAlluxioFiles);

    List<UIFileInfo> fileInfos = new ArrayList<>(inAlluxioFiles.size());
    for (AlluxioURI file : inAlluxioFiles) {
      try {
        long fileId = fileSystemMaster.getFileId(file);
        FileInfo fileInfo = fileSystemMaster.getFileInfo(fileId);
        if (fileInfo != null && fileInfo.getInAlluxioPercentage() == 100) {
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
    request.setAttribute("inAlluxioFileNum", fileInfos.size());

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
