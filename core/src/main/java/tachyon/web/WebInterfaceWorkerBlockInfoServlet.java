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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import tachyon.worker.WorkerStorage;
import tachyon.worker.tiered.StorageDir;

/**
 * Servlet that provides data for displaying block info of a worker.
 */
public class WebInterfaceWorkerBlockInfoServlet extends HttpServlet {
  private static final long serialVersionUID = 4148506607369321012L;
  private final transient WorkerStorage mWorkerStorage;
  private final TachyonConf mTachyonConf;

  public WebInterfaceWorkerBlockInfoServlet(WorkerStorage workerStorage, TachyonConf conf) {
    mWorkerStorage = workerStorage;
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
    Map<Integer, UiFileInfo> uiFileInfoMap = getUiFileInfoMap(tachyonClient);
    List<UiFileInfo> uiFileInfos = new ArrayList<UiFileInfo>(uiFileInfoMap.values());
    Collections.sort(uiFileInfos, UiFileInfo.PATH_STRING_COMPARE);

    String filePath = request.getParameter("path");
    if (!(filePath == null || filePath.isEmpty())) {
      // Display file block info
      try {
        ClientFileInfo fileInfo = tachyonClient.getFileStatus(-1, new TachyonURI(filePath));
        if (fileInfo == null || !uiFileInfoMap.containsKey(fileInfo.getId())) {
          throw new FileDoesNotExistException(filePath);
        }
        UiFileInfo uiFileInfo = uiFileInfoMap.get(fileInfo.getId());
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

    request.setAttribute("nTotalFile", uiFileInfoMap.size());

    // URL can not determine offset and limit, let javascript in jsp determine and redirect
    if (request.getParameter("offset") == null && request.getParameter("limit") == null) {
      getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request, response);
      return;
    }

    try {
      int offset = Integer.parseInt(request.getParameter("offset"));
      int limit = Integer.parseInt(request.getParameter("limit"));
      List<UiFileInfo> sub = uiFileInfos.subList(offset, offset + limit);
      request.setAttribute("fileInfos", sub);
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

  private Map<Integer, UiFileInfo> getUiFileInfoMap(TachyonFS tachyonClient) throws IOException {
    Map<Integer, UiFileInfo> uiFileInfoMap = new HashMap<Integer, UiFileInfo>();
    StorageDir[] storageDirs = mWorkerStorage.getStorageDirs();
    for (StorageDir storageDir : storageDirs) {
      StorageLevelAlias storageLevelAlias =
          StorageDirId.getStorageLevelAlias(storageDir.getStorageDirId());
      Set<Map.Entry<Long, Long>> blockSizes = storageDir.getBlockSizes();
      for (Map.Entry<Long, Long> entry : blockSizes) {
        long blockId = entry.getKey();
        long blockSize = entry.getValue();
        long blockLastAccessTimeMs = storageDir.getLastBlockAccessTimeMs(blockId);
        int fileId = BlockInfo.computeInodeId(blockId);
        if (!uiFileInfoMap.containsKey(fileId)) {
          ClientFileInfo fileInfo = tachyonClient.getFileStatus(fileId, true);
          if (fileInfo != null) {
            uiFileInfoMap.put(fileId, new UiFileInfo(fileInfo));
          }
        }
        UiFileInfo uiFileInfo = uiFileInfoMap.get(fileId);
        if (uiFileInfo != null) {
          uiFileInfo.addBlock(storageLevelAlias, blockId, blockSize, blockLastAccessTimeMs);
        }
      }
    }
    return uiFileInfoMap;
  }
}
