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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFile;
import tachyon.client.TachyonFS;
import tachyon.conf.TachyonConf;
import tachyon.master.BlockInfo;
import tachyon.master.MasterInfo;
import tachyon.thrift.AccessControlException;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.util.CommonUtils;

/**
 * Servlet that provides data for browsing the file system.
 */
public class WebInterfaceBrowseServlet extends HttpServlet {
  /**
   * Class to make referencing file objects more intuitive. Mainly to avoid implicit association by
   * array indexes.
   */
  public static final class UiBlockInfo {
    private final long mId;
    private final long mBlockLength;
    private final boolean mInMemory;

    public UiBlockInfo(BlockInfo blockInfo) {
      mId = blockInfo.mBlockId;
      mBlockLength = blockInfo.mLength;
      mInMemory = blockInfo.isInMemory();
    }

    public long getBlockLength() {
      return mBlockLength;
    }

    public long getID() {
      return mId;
    }

    public boolean inMemory() {
      return mInMemory;
    }
  }

  private static final long serialVersionUID = 6121623049981468871L;

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final transient MasterInfo mMasterInfo;

  public WebInterfaceBrowseServlet(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
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
      throws FileDoesNotExistException, InvalidPathException, IOException {
    String masterAddress =
        Constants.HEADER + mMasterInfo.getMasterAddress().getHostName() + ":"
            + mMasterInfo.getMasterAddress().getPort();
    TachyonFS tachyonClient = TachyonFS.get(new TachyonURI(masterAddress), new TachyonConf());
    TachyonFile tFile = tachyonClient.getFile(path);
    String fileData = null;
    if (tFile == null) {
      throw new FileDoesNotExistException(path.toString());
    }
    if (tFile.isComplete()) {
      InStream is = tFile.getInStream(ReadType.NO_CACHE);
      try {
        int len = (int) Math.min(5 * Constants.KB, tFile.length() - offset);
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
            fileData = CommonUtils.convertByteArrayToStringWithoutEscape(data, 0, read);
            if (fileData == null) {
              fileData = "The requested file is not completely encoded in ascii";
            }
          }
        }
      } finally {
        is.close();
      }
    } else {
      fileData = "The requested file is not complete yet.";
    }
    try {
      tachyonClient.close();
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    List<BlockInfo> rawBlockList = mMasterInfo.getBlockList(path);
    List<UiBlockInfo> uiBlockInfo = new ArrayList<UiBlockInfo>();
    for (BlockInfo blockInfo : rawBlockList) {
      uiBlockInfo.add(new UiBlockInfo(blockInfo));
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
   * @throws UnknownHostException
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException, UnknownHostException {
    request.setAttribute("debug", Constants.DEBUG);

    request.setAttribute("masterNodeAddress", mMasterInfo.getMasterAddress().toString());
    request.setAttribute("invalidPathError", "");
    List<ClientFileInfo> filesInfo = null;
    String requestPath = request.getParameter("path");
    if (requestPath == null || requestPath.isEmpty()) {
      requestPath = TachyonURI.SEPARATOR;
    }
    TachyonURI currentPath = new TachyonURI(requestPath);
    request.setAttribute("currentPath", currentPath.toString());
    request.setAttribute("viewingOffset", 0);

    try {
      ClientFileInfo clientFileInfo = mMasterInfo.getClientFileInfo(currentPath);
      UiFileInfo currentFileInfo = new UiFileInfo(clientFileInfo);
      if (null == currentFileInfo.getAbsolutePath()) {
        throw new FileDoesNotExistException(currentPath.toString());
      }
      request.setAttribute("currentDirectory", currentFileInfo);
      request.setAttribute("blockSizeByte", currentFileInfo.getBlockSizeBytes());
      if (!currentFileInfo.getIsDirectory()) {
        String tmpParam = request.getParameter("offset");
        long offset = 0;
        try {
          if (tmpParam != null) {
            offset = Long.valueOf(tmpParam);
          }
        } catch (NumberFormatException nfe) {
          offset = 0;
        }
        if (offset < 0) {
          offset = 0;
        } else if (offset > clientFileInfo.getLength()) {
          offset = clientFileInfo.getLength();
        }
        displayFile(new TachyonURI(currentFileInfo.getAbsolutePath()), request, offset);
        request.setAttribute("viewingOffset", offset);
        getServletContext().getRequestDispatcher("/viewFile.jsp").forward(request, response);
        return;
      }
      setPathDirectories(currentPath, request);
      filesInfo = mMasterInfo.getFilesInfo(currentPath);
    } catch (FileDoesNotExistException fdne) {
      request.setAttribute("invalidPathError", "Error: Invalid Path " + fdne.getMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    } catch (InvalidPathException ipe) {
      request.setAttribute("invalidPathError", "Error: Invalid Path " + ipe.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    } catch (IOException ie) {
      request.setAttribute("invalidPathError", "Error: File " + currentPath + " is not available "
          + ie.getMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    } catch (AccessControlException ace) {
      request.setAttribute("fatalError", "Error: Access denied " + ace.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    }

    List<UiFileInfo> fileInfos = new ArrayList<UiFileInfo>(filesInfo.size());
    for (ClientFileInfo fileInfo : filesInfo) {
      UiFileInfo toAdd = new UiFileInfo(fileInfo);
      try {
        if (!toAdd.getIsDirectory() && fileInfo.getLength() > 0) {
          toAdd.setFileLocations(mMasterInfo.getFileBlocks(toAdd.getId()).get(0).getLocations());
        }
      } catch (FileDoesNotExistException fdne) {
        request.setAttribute("invalidPathError", "Error: Invalid Path " + fdne.getMessage());
        getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
        return;
      }
      fileInfos.add(toAdd);
    }
    Collections.sort(fileInfos, UiFileInfo.PATH_STRING_COMPARE);

    request.setAttribute("nTotalFile", Integer.valueOf(fileInfos.size()));

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
      throws FileDoesNotExistException, AccessControlException, InvalidPathException {
    if (path.isRoot()) {
      request.setAttribute("pathInfos", new UiFileInfo[0]);
      return;
    }

    String[] splitPath = CommonUtils.getPathComponents(path.toString());
    UiFileInfo[] pathInfos = new UiFileInfo[splitPath.length - 1];
    TachyonURI currentPath = new TachyonURI(TachyonURI.SEPARATOR);
    pathInfos[0] = new UiFileInfo(mMasterInfo.getClientFileInfo(currentPath));
    for (int i = 1; i < splitPath.length - 1; i ++) {
      currentPath = currentPath.join(splitPath[i]);
      pathInfos[i] = new UiFileInfo(mMasterInfo.getClientFileInfo(currentPath));
    }
    request.setAttribute("pathInfos", pathInfos);
  }
}
