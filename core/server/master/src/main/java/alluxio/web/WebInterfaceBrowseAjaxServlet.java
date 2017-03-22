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
import alluxio.web.entity.PageResultEntity;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.WorkerNetAddress;

import org.codehaus.jackson.map.ObjectMapper;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Servlet that provides ajax request for browsing the file system.
 */
@ThreadSafe
public final class WebInterfaceBrowseAjaxServlet extends HttpServlet {

  private static final long serialVersionUID = 7251644350146133763L;
  private final transient AlluxioMasterService mMaster;

  /**
   * Creates a new instance of {@link WebInterfaceBrowseLogAjaxServlet}.
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
    PageResultEntity pageResultEntity = new PageResultEntity();
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");

    ObjectMapper mapper = new ObjectMapper();

    String requestPath = request.getParameter("path");
    List<UIFileInfo> fileInfos = new ArrayList<>();
    if (SecurityUtils.isSecurityEnabled() && AuthenticatedClientUser.get() == null) {
      AuthenticatedClientUser.set(LoginUser.get().getName());
    }
    pageResultEntity.getArgumentMap().put("debug", Configuration.getBoolean(PropertyKey.DEBUG));
    pageResultEntity.getArgumentMap().put("showPermissions",
        Configuration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED));

    pageResultEntity.getArgumentMap().put("masterNodeAddress",
        mMaster.getRpcAddress().toString());
    pageResultEntity.getArgumentMap().put("invalidPathError", "");
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
      pageResultEntity.getArgumentMap().put("invalidPathError",
          "Error: Invalid Path " + e.getMessage());
      response.getWriter().write(mapper.writeValueAsString(pageResultEntity));
      return;
    } catch (InvalidPathException e) {
      pageResultEntity.getArgumentMap().put("invalidPathError",
          "Error: Invalid Path " + e.getLocalizedMessage());
      response.getWriter().write(mapper.writeValueAsString(pageResultEntity));
      return;
    } catch (AccessControlException e) {
      pageResultEntity.getArgumentMap().put("invalidPathError",
          "Error: File " + currentPath + " cannot be accessed " + e.getMessage());
      response.getWriter().write(mapper.writeValueAsString(pageResultEntity));
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
        pageResultEntity.getArgumentMap().put("FileDoesNotExistException",
            "Error: non-existing file " + e.getMessage());
        response.getWriter().write(mapper.writeValueAsString(pageResultEntity));
        return;
      } catch (InvalidPathException e) {
        pageResultEntity.getArgumentMap().put("InvalidPathException",
            "Error: invalid path " + e.getMessage());
      } catch (AccessControlException e) {
        pageResultEntity.getArgumentMap().put("AccessControlException",
            "Error: File " + currentPath + " cannot be accessed " + e.getMessage());
        response.getWriter().write(mapper.writeValueAsString(pageResultEntity));
        return;
      }
      fileInfos.add(toAdd);
    }
    pageResultEntity.setPageData(fileInfos);
    pageResultEntity.setTotalCount(fileInfos.size());

    String json = mapper.writeValueAsString(pageResultEntity);
    response.getWriter().write(json);
  }
}
