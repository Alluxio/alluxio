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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import tachyon.StorageDirId;
import tachyon.StorageLevelAlias;
import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.conf.TachyonConf;
import tachyon.master.BlockInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.worker.block.BlockWorker;
import tachyon.worker.block.meta.StorageDir;

/**
 * Servlet that provides data for displaying block info of a worker.
 */
public class WebInterfaceWorkerBlockInfoServlet extends HttpServlet {
  private static final long serialVersionUID = 4148506607369321012L;
  private final transient BlockWorker mWorker;
  private final transient TachyonConf mTachyonConf;

  public WebInterfaceWorkerBlockInfoServlet(BlockWorker worker, TachyonConf conf) {
    mWorker = worker;
    mTachyonConf = conf;
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
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    request.setAttribute("fatalError", "");
    TachyonFS tachyonClient = TachyonFS.get(mTachyonConf);

    String filePath = request.getParameter("path");
    if (!(filePath == null || filePath.isEmpty())) {
      // Display file block info
      try {
        UiFileInfo uiFileInfo = getUiFileInfo(tachyonClient, new TachyonURI(filePath));
        request.setAttribute("fileBlocksOnTier", uiFileInfo.getBlocksOnTier());
        request.setAttribute("blockSizeByte", uiFileInfo.getBlockSizeBytes());
        request.setAttribute("path", filePath);

        getServletContext().getRequestDispatcher("/worker/viewFileBlocks.jsp").forward(request,
            response);
        return;
      } catch (FileDoesNotExistException fdne) {
        request.setAttribute("fatalError", "Error: Invalid Path " + fdne.getMessage());
        getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request,
            response);
        return;
      } catch (IOException ie) {
        request.setAttribute("invalidPathError", "Error: File " + filePath + " is not available "
            + ie.getMessage());
        getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request,
            response);
        return;
      }
    }

    List<Integer> fileIds = getSortedFileIds();
    request.setAttribute("nTotalFile", fileIds.size());

    // URL can not determine offset and limit, let javascript in jsp determine and redirect
    if (request.getParameter("offset") == null && request.getParameter("limit") == null) {
      getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request, response);
      return;
    }

    try {
      int offset = Integer.parseInt(request.getParameter("offset"));
      int limit = Integer.parseInt(request.getParameter("limit"));
      List<Integer> subFileIds = fileIds.subList(offset, offset + limit);
      List<UiFileInfo> uiFileInfos = new ArrayList<UiFileInfo>(subFileIds.size());
      for (int fileId : subFileIds) {
        uiFileInfos.add(getUiFileInfo(tachyonClient, fileId));
      }
      request.setAttribute("fileInfos", uiFileInfos);
    } catch (FileDoesNotExistException fdne) {
      request.setAttribute("fatalError", "Error: Invalid FileId " + fdne.getMessage());
      getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request,
          response);
      return;
    } catch (NumberFormatException nfe) {
      request.setAttribute("fatalError",
          "Error: offset or limit parse error, " + nfe.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request, response);
      return;
    } catch (IndexOutOfBoundsException iobe) {
      request.setAttribute("fatalError", "Error: offset or offset + limit is out of bound, "
          + iobe.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request, response);
      return;
    } catch (IllegalArgumentException iae) {
      request.setAttribute("fatalError", iae.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request, response);
      return;
    }

    getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request, response);
  }

  /***
   * Get sorted fileIds of the files cached in the worker.
   *
   * @return a sorted fileId list
   */
  // TODO: Add this functionality back
  private List<Integer> getSortedFileIds() {
    Set<Integer> fileIds = new HashSet<Integer>();
//    for (StorageDir storageDir : mWorkerStorage.getStorageDirs()) {
//      for (long blockId : storageDir.getBlockIds()) {
//        int fileId = BlockInfo.computeInodeId(blockId);
//        fileIds.add(fileId);
//      }
//    }
    List<Integer> sortedFileIds = new ArrayList<Integer>(fileIds);
    Collections.sort(sortedFileIds);
    return sortedFileIds;
  }

  /***
   * Get the UiFileInfo object based on fileId.
   *
   * @param tachyonClient the TachyonFS client.
   * @param fileId the file id of the file.
   * @return the UiFileInfo object of the file.
   * @throws FileDoesNotExistException
   * @throws IOException
   */
  private UiFileInfo getUiFileInfo(TachyonFS tachyonClient, int fileId)
      throws FileDoesNotExistException, IOException {
    return getUiFileInfo(tachyonClient, fileId, TachyonURI.EMPTY_URI);
  }

  /***
   * Get the UiFileInfo object based on filePath.
   *
   * @param tachyonClient the TachyonFS client.
   * @param filePath the path of the file.
   * @return the UiFileInfo object of the file.
   * @throws FileDoesNotExistException
   * @throws IOException
   */
  private UiFileInfo getUiFileInfo(TachyonFS tachyonClient, TachyonURI filePath)
      throws FileDoesNotExistException, IOException {
    return getUiFileInfo(tachyonClient, -1, filePath);
  }

  /**
   * Gets the UiFileInfo object that represents the fileId, or the filePath if fileId is -1.
   *
   * @param tachyonClient the TachyonFS client.
   * @param fileId the file id of the file.
   * @param filePath the path of the file. valid iff fileId is -1.
   * @return the UiFileInfo object of the file.
   * @throws FileDoesNotExistException
   * @throws IOException
   */
  // TODO: Add this functionality back
  private UiFileInfo getUiFileInfo(TachyonFS tachyonClient, int fileId, TachyonURI filePath)
      throws FileDoesNotExistException, IOException {
    ClientFileInfo fileInfo = tachyonClient.getFileStatus(fileId, filePath, true);
    if (fileInfo == null) {
      throw new FileDoesNotExistException(fileId != -1 ? Integer.toString(fileId)
          : filePath.toString());
    }

    UiFileInfo uiFileInfo = new UiFileInfo(fileInfo);
    boolean blockExistOnWorker = false;
//    for (long blockId : fileInfo.getBlockIds()) {
//      for (StorageDir storageDir : mWorkerStorage.getStorageDirs()) {
//        if (storageDir.containsBlock(blockId)) {
//          blockExistOnWorker = true;
//          long blockSize = storageDir.getBlockSize(blockId);
//          long blockLastAccessTime = storageDir.getLastBlockAccessTimeMs(blockId);
//          StorageLevelAlias storageLevelAlias =
//              StorageDirId.getStorageLevelAlias(storageDir.getStorageDirId());
//          uiFileInfo.addBlock(storageLevelAlias, blockId, blockSize, blockLastAccessTime);
//          break;
//        }
//      }
//    }
    if (!blockExistOnWorker) {
      throw new FileDoesNotExistException(fileId != -1 ? Integer.toString(fileId)
          : filePath.toString());
    }
    return uiFileInfo;
  }
}
