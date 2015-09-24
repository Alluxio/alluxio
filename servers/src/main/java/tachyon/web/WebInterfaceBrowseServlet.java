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

import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.TachyonStorageType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.options.InStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.master.TachyonMaster;
import tachyon.thrift.BlockLocation;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.NetAddress;
import tachyon.util.io.PathUtils;

/**
 * Servlet that provides data for browsing the file system.
 */
public final class WebInterfaceBrowseServlet extends HttpServlet {

  private static final long serialVersionUID = 6121623049981468871L;

  private final transient TachyonMaster mMaster;
  private final transient TachyonConf mTachyonConf;

  public WebInterfaceBrowseServlet(TachyonMaster master) {
    mMaster = master;
    mTachyonConf = new TachyonConf();
  }

  /**
   * This function displays 5KB of a file from a specific offset if it is in ASCII format.
   *
   * @param path The path of the file to display
   * @param request The HttpServletRequest object
   * @param offset Where the file starts to display.
   * @throws FileDoesNotExistException
   * @throws IOException
   * @throws InvalidPathException
   */
  private void displayFile(TachyonURI path, HttpServletRequest request, long offset)
      throws FileDoesNotExistException, InvalidPathException, IOException, TachyonException {
    TachyonFileSystem tFS = TachyonFileSystem.get();
    TachyonFile tFile = tFS.open(path);
    String fileData = null;
    if (tFile == null) {
      throw new FileDoesNotExistException(path.toString());
    }
    FileInfo fileInfo = tFS.getInfo(tFile);
    if (fileInfo.isComplete) {
      InStreamOptions readNoCache = new InStreamOptions.Builder(mTachyonConf)
          .setTachyonStorageType(TachyonStorageType.NO_STORE).build();
      FileInStream is = tFS.getInStream(tFile, readNoCache);
      try {
        int len = (int) Math.min(5 * Constants.KB, fileInfo.length - offset);
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
            fileData = Utils.convertByteArrayToStringWithoutEscape(data, 0, read);
          }
        }
      } finally {
        is.close();
      }
    } else {
      fileData = "The requested file is not complete yet.";
    }
    List<UiBlockInfo> uiBlockInfo = new ArrayList<UiBlockInfo>();
    for (FileBlockInfo fileBlockInfo : mMaster.getFileSystemMaster().getFileBlockInfoList(path)) {
      uiBlockInfo.add(new UiBlockInfo(fileBlockInfo));
    }
    request.setAttribute("fileBlocks", uiBlockInfo);
    request.setAttribute("fileData", fileData);
  }

  /**
   * Populates attribute fields with data from the MasterInfo associated with this servlet. Errors
   * will be displayed in an error field. Debugging can be enabled to display additional data. Will
   * eventually redirect the request to a jsp.
   *
   * @param request The HttpServletRequest object
   * @param response The HttpServletResponse object
   * @throws ServletException
   * @throws IOException
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    request.setAttribute("debug", Constants.DEBUG);

    request.setAttribute("masterNodeAddress", mMaster.getMasterAddress().toString());
    request.setAttribute("invalidPathError", "");
    List<FileInfo> filesInfo;
    String requestPath = request.getParameter("path");
    if (requestPath == null || requestPath.isEmpty()) {
      requestPath = TachyonURI.SEPARATOR;
    }
    TachyonURI currentPath = new TachyonURI(requestPath);
    request.setAttribute("currentPath", currentPath.toString());
    request.setAttribute("viewingOffset", 0);

    try {
      long fileId = mMaster.getFileSystemMaster().getFileId(currentPath);
      FileInfo fileInfo = mMaster.getFileSystemMaster().getFileInfo(fileId);
      UiFileInfo currentFileInfo = new UiFileInfo(fileInfo);
      if (currentFileInfo.getAbsolutePath() == null) {
        throw new FileDoesNotExistException(currentPath.toString());
      }
      request.setAttribute("currentDirectory", currentFileInfo);
      request.setAttribute("blockSizeBytes", currentFileInfo.getBlockSizeBytes());
      request.setAttribute("workerWebPort", mTachyonConf.getInt(Constants.WORKER_WEB_PORT));
      if (!currentFileInfo.getIsDirectory()) {
        String offsetParam = request.getParameter("offset");
        long relativeOffset = 0;
        long offset;
        try {
          if (offsetParam != null) {
            relativeOffset = Long.valueOf(offsetParam);
          }
        } catch (NumberFormatException nfe) {
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
          displayFile(new TachyonURI(currentFileInfo.getAbsolutePath()), request, offset);
        } catch (TachyonException e) {
          throw new IOException(e.getMessage());
        }
        request.setAttribute("viewingOffset", offset);
        getServletContext().getRequestDispatcher("/viewFile.jsp").forward(request, response);
        return;
      }
      setPathDirectories(currentPath, request);
      fileId = mMaster.getFileSystemMaster().getFileId(currentPath);
      filesInfo = mMaster.getFileSystemMaster().getFileInfoList(fileId);
    } catch (FileDoesNotExistException fdne) {
      request.setAttribute("invalidPathError", "Error: Invalid Path " + fdne.getMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    } catch (InvalidPathException ipe) {
      request.setAttribute("invalidPathError", "Error: Invalid Path " + ipe.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    } catch (IOException ie) {
      request.setAttribute("invalidPathError",
          "Error: File " + currentPath + " is not available " + ie.getMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    }

    List<UiFileInfo> fileInfos = new ArrayList<UiFileInfo>(filesInfo.size());
    for (FileInfo fileInfo : filesInfo) {
      UiFileInfo toAdd = new UiFileInfo(fileInfo);
      try {
        if (!toAdd.getIsDirectory() && fileInfo.getLength() > 0) {
          FileBlockInfo blockInfo =
              mMaster.getFileSystemMaster().getFileBlockInfoList(toAdd.getId()).get(0);
          List<NetAddress> addrs = Lists.newArrayList();
          // add the in-memory block locations
          for (BlockLocation location : blockInfo.getBlockInfo().getLocations()) {
            addrs.add(location.getWorkerAddress());
          }
          // add underFS locations
          addrs.addAll(blockInfo.getUfsLocations());
          toAdd.setFileLocations(addrs);
        }
      } catch (FileDoesNotExistException fdne) {
        request.setAttribute("FileDoesNotExistException",
            "Error: non-existing file " + fdne.getMessage());
        getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
        return;
      }
      fileInfos.add(toAdd);
    }
    Collections.sort(fileInfos, UiFileInfo.PATH_STRING_COMPARE);

    request.setAttribute("nTotalFile", fileInfos.size());

    // URL can not determine offset and limit, let javascript in jsp determine and redirect
    if (request.getParameter("offset") == null && request.getParameter("limit") == null) {
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
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
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    } catch (IndexOutOfBoundsException iobe) {
      request.setAttribute("fatalError",
          "Error: offset or offset + limit is out of bound, " + iobe.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    } catch (IllegalArgumentException iae) {
      request.setAttribute("fatalError", iae.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    }

    getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
  }

  /**
   * This function sets the fileinfos for folders that are in the path to the current directory.
   *
   * @param path The path of the current directory.
   * @param request The HttpServletRequest object
   * @throws FileDoesNotExistException
   * @throws InvalidPathException
   */
  private void setPathDirectories(TachyonURI path, HttpServletRequest request)
      throws FileDoesNotExistException, InvalidPathException {
    if (path.isRoot()) {
      request.setAttribute("pathInfos", new UiFileInfo[0]);
      return;
    }

    String[] splitPath = PathUtils.getPathComponents(path.toString());
    UiFileInfo[] pathInfos = new UiFileInfo[splitPath.length - 1];
    TachyonURI currentPath = new TachyonURI(TachyonURI.SEPARATOR);
    long fileId = mMaster.getFileSystemMaster().getFileId(currentPath);
    pathInfos[0] = new UiFileInfo(mMaster.getFileSystemMaster().getFileInfo(fileId));
    for (int i = 1; i < splitPath.length - 1; i ++) {
      currentPath = currentPath.join(splitPath[i]);
      fileId = mMaster.getFileSystemMaster().getFileId(currentPath);
      pathInfos[i] = new UiFileInfo(mMaster.getFileSystemMaster().getFileInfo(fileId));
    }
    request.setAttribute("pathInfos", pathInfos);
  }
}
