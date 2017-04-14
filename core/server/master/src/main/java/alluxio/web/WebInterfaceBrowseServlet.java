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
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.AlluxioMasterService;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.security.LoginUser;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.SecurityUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.WorkerNetAddress;

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
 * Servlet that provides data for browsing the file system.
 */
@ThreadSafe
public final class WebInterfaceBrowseServlet extends HttpServlet {

  private static final long serialVersionUID = 6121623049981468871L;

  private final transient AlluxioMasterService mMaster;

  /**
   * Creates a new instance of {@link WebInterfaceBrowseServlet}.
   *
   * @param master the Alluxio master
   */
  public WebInterfaceBrowseServlet(AlluxioMasterService master) {
    mMaster = master;
  }

  /**
   * This function displays 5KB of a file from a specific offset if it is in ASCII format.
   *
   * @param path the path of the file to display
   * @param request the {@link HttpServletRequest} object
   * @param offset where the file starts to display
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   * @throws InvalidPathException if an invalid path is encountered
   * @throws AlluxioException if an unexpected Alluxio exception is thrown
   */
  private void displayFile(AlluxioURI path, HttpServletRequest request, long offset)
      throws FileDoesNotExistException, InvalidPathException, IOException, AlluxioException {
    FileSystem fs = FileSystem.Factory.get();
    String fileData;
    URIStatus status = fs.getStatus(path);
    if (status.isCompleted()) {
      OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
      try (FileInStream is = fs.openFile(path, options)) {
        int len = (int) Math.min(5 * Constants.KB, status.getLength() - offset);
        byte[] data = new byte[len];
        long skipped = is.skip(offset);
        if (skipped < 0) {
          // nothing was skipped
          fileData = "Unable to traverse to offset; is file empty?";
        } else if (skipped < offset) {
          // couldn't skip all the way to offset
          fileData = "Unable to traverse to offset; is offset larger than the file?";
        } else {
          // read may not read up to len, so only convert what was read
          int read = is.read(data, 0, len);
          if (read < 0) {
            // stream couldn't read anything, skip went to EOF?
            fileData = "Unable to read file";
          } else {
            fileData = WebUtils.convertByteArrayToStringWithoutEscape(data, 0, read);
          }
        }
      }
    } else {
      fileData = "The requested file is not complete yet.";
    }
    List<UIFileBlockInfo> uiBlockInfo = new ArrayList<>();
    for (FileBlockInfo fileBlockInfo : mMaster.getMaster(FileSystemMaster.class)
        .getFileBlockInfoList(path)) {
      uiBlockInfo.add(new UIFileBlockInfo(fileBlockInfo));
    }
    request.setAttribute("fileBlocks", uiBlockInfo);
    request.setAttribute("fileData", fileData);
    request.setAttribute("highestTierAlias",
        mMaster.getMaster(BlockMaster.class).getGlobalStorageTierAssoc().getAlias(0));
  }

  /**
   * Populates attribute fields with data from the MasterInfo associated with this servlet. Errors
   * will be displayed in an error field. Debugging can be enabled to display additional data. Will
   * eventually redirect the request to a jsp.
   *
   * @param request the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   * @throws ServletException if the target resource throws this exception
   * @throws IOException if the target resource throws this exception
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    if (SecurityUtils.isSecurityEnabled() && AuthenticatedClientUser.get() == null) {
      AuthenticatedClientUser.set(LoginUser.get().getName());
    }
    request.setAttribute("debug", Configuration.getBoolean(PropertyKey.DEBUG));
    request.setAttribute("showPermissions",
        Configuration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED));

    request.setAttribute("masterNodeAddress", mMaster.getRpcAddress().toString());
    request.setAttribute("invalidPathError", "");
    List<FileInfo> filesInfo;
    String requestPath = request.getParameter("path");
    if (requestPath == null || requestPath.isEmpty()) {
      requestPath = AlluxioURI.SEPARATOR;
    }
    AlluxioURI currentPath = new AlluxioURI(requestPath);
    request.setAttribute("currentPath", currentPath.toString());
    request.setAttribute("viewingOffset", 0);
    FileSystemMaster fileSystemMaster = mMaster.getMaster(FileSystemMaster.class);

    try {
      long fileId = fileSystemMaster.getFileId(currentPath);
      FileInfo fileInfo = fileSystemMaster.getFileInfo(fileId);
      UIFileInfo currentFileInfo = new UIFileInfo(fileInfo);
      if (currentFileInfo.getAbsolutePath() == null) {
        throw new FileDoesNotExistException(currentPath.toString());
      }
      request.setAttribute("currentDirectory", currentFileInfo);
      request.setAttribute("blockSizeBytes", currentFileInfo.getBlockSizeBytes());
      if (!currentFileInfo.getIsDirectory()) {
        String offsetParam = request.getParameter("offset");
        long relativeOffset = 0;
        long offset;
        try {
          if (offsetParam != null) {
            relativeOffset = Long.parseLong(offsetParam);
          }
        } catch (NumberFormatException e) {
          relativeOffset = 0;
        }
        String endParam = request.getParameter("end");
        // If no param "end" presents, the offset is relative to the beginning; otherwise, it is
        // relative to the end of the file.
        if (endParam == null) {
          offset = relativeOffset;
        } else {
          offset = fileInfo.getLength() - relativeOffset;
        }
        if (offset < 0) {
          offset = 0;
        } else if (offset > fileInfo.getLength()) {
          offset = fileInfo.getLength();
        }
        try {
          displayFile(new AlluxioURI(currentFileInfo.getAbsolutePath()), request, offset);
        } catch (AlluxioException e) {
          throw new IOException(e);
        }
        request.setAttribute("viewingOffset", offset);
        getServletContext().getRequestDispatcher("/viewFile.jsp").forward(request, response);
        return;
      }
      setPathDirectories(currentPath, request);
      filesInfo = fileSystemMaster.listStatus(currentPath,
          ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Always));
    } catch (FileDoesNotExistException e) {
      request.setAttribute("invalidPathError", "Error: Invalid Path " + e.getMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    } catch (InvalidPathException e) {
      request.setAttribute("invalidPathError", "Error: Invalid Path " + e.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    } catch (IOException e) {
      request.setAttribute("invalidPathError",
          "Error: File " + currentPath + " is not available " + e.getMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    } catch (AccessControlException e) {
      request.setAttribute("invalidPathError",
          "Error: File " + currentPath + " cannot be accessed " + e.getMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    }

    List<UIFileInfo> fileInfos = new ArrayList<>(filesInfo.size());
    for (FileInfo fileInfo : filesInfo) {
      UIFileInfo toAdd = new UIFileInfo(fileInfo);
      try {
        if (!toAdd.getIsDirectory() && fileInfo.getLength() > 0) {
          FileBlockInfo blockInfo =
              fileSystemMaster.getFileBlockInfoList(new AlluxioURI(toAdd.getAbsolutePath())).get(0);
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
    Collections.sort(fileInfos, UIFileInfo.PATH_STRING_COMPARE);

    request.setAttribute("nTotalFile", fileInfos.size());

    // URL can not determine offset and limit, let javascript in jsp determine and redirect
    if (request.getParameter("offset") == null && request.getParameter("limit") == null) {
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
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
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    } catch (IndexOutOfBoundsException e) {
      request.setAttribute("fatalError",
          "Error: offset or offset + limit is out of bound, " + e.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    } catch (IllegalArgumentException e) {
      request.setAttribute("fatalError", e.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    }

    getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
  }

  /**
   * This function sets the file information for directories that are in the path to the current
   * directory.
   *
   * @param path the path of the current directory
   * @param request the {@link HttpServletRequest} object
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws AccessControlException if permission checking fails
   */
  private void setPathDirectories(AlluxioURI path, HttpServletRequest request)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    FileSystemMaster fileSystemMaster = mMaster.getMaster(FileSystemMaster.class);
    if (path.isRoot()) {
      request.setAttribute("pathInfos", new UIFileInfo[0]);
      return;
    }

    String[] splitPath = PathUtils.getPathComponents(path.toString());
    UIFileInfo[] pathInfos = new UIFileInfo[splitPath.length - 1];
    AlluxioURI currentPath = new AlluxioURI(AlluxioURI.SEPARATOR);
    long fileId = fileSystemMaster.getFileId(currentPath);
    pathInfos[0] = new UIFileInfo(fileSystemMaster.getFileInfo(fileId));
    for (int i = 1; i < splitPath.length - 1; i++) {
      currentPath = currentPath.join(splitPath[i]);
      fileId = fileSystemMaster.getFileId(currentPath);
      pathInfos[i] = new UIFileInfo(fileSystemMaster.getFileInfo(fileId));
    }
    request.setAttribute("pathInfos", pathInfos);
  }
}
