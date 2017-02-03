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
import alluxio.exception.InvalidPathException;
import alluxio.master.AlluxioMasterService;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.security.LoginUser;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.SecurityUtils;

import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.WorkerNetAddress;

import com.alibaba.fastjson.JSON;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Servlet that provides ajax request for browsing the file system.
 */
@ThreadSafe
public final class WebInterfaceBrowseAjaxServlet extends HttpServlet {

  private static final long serialVersionUID = 7251644350146133763L;
  private final transient AlluxioMasterService mMaster;
  private static final FilenameFilter LOG_FILE_FILTER = new FilenameFilter() {
    @Override
    public boolean accept(File dir, String name) {
      return name.toLowerCase().endsWith(".log");
    }
  };

  /**
   * Creates a new instance of {@link WebInterfaceBrowseAjaxServlet}.
   *
   * @param master the Alluxio master
   */
  public WebInterfaceBrowseAjaxServlet(AlluxioMasterService master) {
    mMaster = master;
  }

  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws
      ServletException, IOException {
    doGet(req, resp);
  }

  /**
   * Populates attribute fields with data from the MasterInfo associated with this servlet. Errors
   * will be displayed in an error field. Debugging can be enabled to display additional data. Will
   * eventually redirect the request to a jsp.
   *
   * @param request  the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   * @throws ServletException if the target resource throws this exception
   * @throws IOException      if the target resource throws this exception
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String requestPath = request.getParameter("path");
    List<UIFileInfo> fileInfos = new ArrayList<>();
    String baseUrl = request.getParameter("baseUrl");
    if (baseUrl != null && baseUrl.contains("browseLogs")) {
      request.setAttribute("debug", Configuration.getBoolean(PropertyKey.DEBUG));
      request.setAttribute("invalidPathError", "");
      request.setAttribute("viewingOffset", 0);
      request.setAttribute("downloadLogFile", 1);
      request.setAttribute("baseUrl", "./browseLogs");
      request.setAttribute("currentPath", "");
      request.setAttribute("showPermissions", false);
      String logsPath = Configuration.get(PropertyKey.LOGS_DIR);
      File logsDir = new File(logsPath);
      if (requestPath == null || requestPath.isEmpty()) {
        // List all log files in the log/ directory.
        File[] logFiles = logsDir.listFiles(LOG_FILE_FILTER);
        if (logFiles != null) {
          for (File logFile : logFiles) {
            String logFileName = logFile.getName();
            fileInfos.add(new UIFileInfo(new UIFileInfo.LocalFileInfo(logFileName, logFileName,
                logFile.length(), UIFileInfo.LocalFileInfo.EMPTY_CREATION_TIME,
                logFile.lastModified(), logFile.isDirectory())));
          }
        }
        Collections.sort(fileInfos, UIFileInfo.PATH_STRING_COMPARE);
      }
    } else {
      if (SecurityUtils.isSecurityEnabled() && AuthenticatedClientUser.get() == null) {
        AuthenticatedClientUser.set(LoginUser.get().getName());
      }
      request.setAttribute("debug", Configuration.getBoolean(PropertyKey.DEBUG));
      request.setAttribute("showPermissions",
          Configuration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED));
      request.setAttribute("masterNodeAddress", mMaster.getRpcAddress().toString());
      request.setAttribute("invalidPathError", "");
      if (requestPath == null || requestPath.isEmpty()) {
        requestPath = AlluxioURI.SEPARATOR;
      }
      List<FileInfo> filesInfo;
      AlluxioURI currentPath = new AlluxioURI(requestPath);
      try {
        long fileId = mMaster.getFileSystemMaster().getFileId(currentPath);
        FileInfo fileInfo = mMaster.getFileSystemMaster().getFileInfo(fileId);
        UIFileInfo currentFileInfo = new UIFileInfo(fileInfo);
        if (currentFileInfo.getAbsolutePath() == null) {
          throw new FileDoesNotExistException(currentPath.toString());
        }
        filesInfo = mMaster.getFileSystemMaster().listStatus(currentPath,
            ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Always));
      } catch (FileDoesNotExistException e) {
        request.setAttribute("invalidPathError", "Error: Invalid Path " + e.getMessage());
        getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
        return;
      } catch (InvalidPathException e) {
        request.setAttribute("invalidPathError", "Error: Invalid Path "
            + e.getLocalizedMessage());
        getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
        return;
      } catch (AccessControlException e) {
        request.setAttribute("invalidPathError",
            "Error: File " + currentPath + " cannot be accessed " + e.getMessage());
        getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
        return;
      }
      fileInfos = new ArrayList<>(filesInfo.size());
      for (FileInfo fileInfo : filesInfo) {
        UIFileInfo toAdd = new UIFileInfo(fileInfo);
        try {
          if (!toAdd.getIsDirectory() && fileInfo.getLength() > 0) {
            FileBlockInfo blockInfo =
                mMaster.getFileSystemMaster()
                    .getFileBlockInfoList(new AlluxioURI(toAdd.getAbsolutePath())).get(0);
            List<String> locations = new ArrayList<>();
            // add the in-memory block locations
            for (BlockLocation location : blockInfo.getBlockInfo().getLocations()) {
              WorkerNetAddress address = location.getWorkerAddress();
              locations.add(address.getHost() + ":" + address.getDataPort());
            }
            // add underFS locations
            locations.addAll(blockInfo.getUfsLocations());
            toAdd.setFileLocations(locations);
          }
        } catch (FileDoesNotExistException e) {
          request.setAttribute("FileDoesNotExistException",
              "Error: non-existing file " + e.getMessage());
          getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
          return;
        } catch (InvalidPathException e) {
          request.setAttribute("InvalidPathException",
              "Error: invalid path " + e.getMessage());
          getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
        } catch (AccessControlException e) {
          request.setAttribute("AccessControlException",
              "Error: File " + currentPath + " cannot be accessed " + e.getMessage());
          getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
          return;
        }
        fileInfos.add(toAdd);
      }
    }
    String json = JSON.toJSONString(fileInfos);
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    response.getWriter().write(json);
  }
}
