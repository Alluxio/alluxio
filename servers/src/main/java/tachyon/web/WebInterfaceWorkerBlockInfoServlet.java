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

import com.google.common.base.Preconditions;

import tachyon.TachyonURI;
import tachyon.WorkerStorageTierAssoc;
import tachyon.client.file.FileSystem;
import tachyon.client.file.FileSystemContext;
import tachyon.client.file.FileSystemMasterClient;
import tachyon.client.file.URIStatus;
import tachyon.exception.BlockDoesNotExistException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.exception.TachyonException;
import tachyon.master.block.BlockId;
import tachyon.worker.WorkerContext;
import tachyon.worker.block.BlockDataManager;
import tachyon.worker.block.BlockStoreMeta;
import tachyon.worker.block.meta.BlockMeta;

/**
 * Servlet that provides data for displaying block info of a worker.
 */
public final class WebInterfaceWorkerBlockInfoServlet extends HttpServlet {
  private static final long serialVersionUID = 4148506607369321012L;
  private final transient BlockDataManager mBlockDataManager;

  /**
   * Creates a new instance of {@link WebInterfaceWorkerBlockInfoServlet}.
   *
   * @param blockDataManager block data manager
   */
  public WebInterfaceWorkerBlockInfoServlet(BlockDataManager blockDataManager) {
    mBlockDataManager = Preconditions.checkNotNull(blockDataManager);
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
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    request.setAttribute("fatalError", "");
    FileSystem tFS = FileSystem.Factory.get();
    String filePath = request.getParameter("path");
    if (!(filePath == null || filePath.isEmpty())) {
      // Display file block info
      try {
        UIFileInfo uiFileInfo = getUiFileInfo(tFS, new TachyonURI(filePath));
        request.setAttribute("fileBlocksOnTier", uiFileInfo.getBlocksOnTier());
        request.setAttribute("blockSizeBytes", uiFileInfo.getBlockSizeBytes());
        request.setAttribute("path", filePath);
        getServletContext().getRequestDispatcher("/worker/viewFileBlocks.jsp").forward(request,
            response);
        return;
      } catch (FileDoesNotExistException e) {
        request.setAttribute("fatalError", "Error: Invalid Path " + e.getMessage());
        getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request,
            response);
        return;
      } catch (IOException e) {
        request.setAttribute("invalidPathError",
            "Error: File " + filePath + " is not available " + e.getMessage());
        getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request,
            response);
        return;
      } catch (BlockDoesNotExistException e) {
        request.setAttribute("fatalError", "Error: block not found. " + e.getMessage());
        getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request,
            response);
        return;
      } catch (TachyonException e) {
        request.setAttribute("fatalError", "Error: tachyon exception. " + e.getMessage());
        getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request,
            response);
        return;
      }
    }

    List<Long> fileIds = getSortedFileIds();
    request.setAttribute("nTotalFile", fileIds.size());

    request.setAttribute("orderedTierAliases",
        new WorkerStorageTierAssoc(WorkerContext.getConf()).getOrderedStorageAliases());

    // URL can not determine offset and limit, let javascript in jsp determine and redirect
    if (request.getParameter("offset") == null && request.getParameter("limit") == null) {
      getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request, response);
      return;
    }

    try {
      int offset = Integer.parseInt(request.getParameter("offset"));
      int limit = Integer.parseInt(request.getParameter("limit"));
      List<Long> subFileIds = fileIds.subList(offset, offset + limit);
      List<UIFileInfo> uiFileInfos = new ArrayList<UIFileInfo>(subFileIds.size());
      for (long fileId : subFileIds) {
        uiFileInfos.add(getUiFileInfo(tFS, fileId));
      }
      request.setAttribute("fileInfos", uiFileInfos);
    } catch (FileDoesNotExistException e) {
      request.setAttribute("fatalError", "Error: Invalid FileId " + e.getMessage());
      getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request, response);
      return;
    } catch (NumberFormatException e) {
      request.setAttribute("fatalError",
          "Error: offset or limit parse error, " + e.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request, response);
      return;
    } catch (IndexOutOfBoundsException e) {
      request.setAttribute("fatalError",
          "Error: offset or offset + limit is out of bound, " + e.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request, response);
      return;
    } catch (IllegalArgumentException e) {
      request.setAttribute("fatalError", e.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request, response);
      return;
    } catch (BlockDoesNotExistException e) {
      request.setAttribute("fatalError", e.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request, response);
      return;
    } catch (TachyonException e) {
      request.setAttribute("fatalError", e.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request, response);
      return;
    }

    getServletContext().getRequestDispatcher("/worker/blockInfo.jsp").forward(request, response);
  }

  /***
   * Gets sorted file ids of the files cached in the worker.
   *
   * @return a sorted file id list
   */
  private List<Long> getSortedFileIds() {
    Set<Long> fileIds = new HashSet<Long>();
    BlockStoreMeta storeMeta = mBlockDataManager.getStoreMeta();
    for (List<Long> blockIds : storeMeta.getBlockList().values()) {
      for (long blockId : blockIds) {
        long fileId =
            BlockId.createBlockId(BlockId.getContainerId(blockId), BlockId.getMaxSequenceNumber());
        fileIds.add(fileId);
      }
    }
    List<Long> sortedFileIds = new ArrayList<Long>(fileIds);
    Collections.sort(sortedFileIds);
    return sortedFileIds;
  }

  /***
   * Gets the {@link UIFileInfo} object based on file id.
   *
   * @param fileSystem the {@link FileSystem} client
   * @param fileId the file id of the file
   * @return the {@link UIFileInfo} object of the file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   */
  private UIFileInfo getUiFileInfo(FileSystem fileSystem, long fileId)
      throws FileDoesNotExistException, BlockDoesNotExistException, IOException, TachyonException {
    // TODO(calvin): Remove this dependency
    FileSystemMasterClient masterClient = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      return getUiFileInfo(fileSystem, new TachyonURI(masterClient.getStatusInternal(fileId)
          .getPath()));
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(masterClient);
    }
  }

  /**
   * Gets the {@link UIFileInfo} object that represents the file id, or the file path if file id is
   * -1.
   *
   * @param fileSystem the {@link FileSystem} client
   * @param filePath the path of the file. valid iff fileId is -1
   * @return the {@link UIFileInfo} object of the file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   */
  private UIFileInfo getUiFileInfo(FileSystem fileSystem, TachyonURI filePath)
      throws BlockDoesNotExistException, FileDoesNotExistException, InvalidPathException,
      IOException, TachyonException {
    URIStatus status;
    try {
      status = fileSystem.getStatus(filePath);
    } catch (TachyonException e) {
      throw new FileDoesNotExistException(filePath.toString());
    }
    UIFileInfo uiFileInfo = new UIFileInfo(status.getInfo());
    boolean blockExistOnWorker = false;
    for (long blockId : status.getBlockIds()) {
      if (mBlockDataManager.hasBlockMeta(blockId)) {
        blockExistOnWorker = true;
        BlockMeta blockMeta = mBlockDataManager.getVolatileBlockMeta(blockId);
        long blockSize = blockMeta.getBlockSize();
        // The block last access time is not available. Use -1 for now.
        // It's not necessary to show location information here since
        // we are viewing at the context of this worker.
        uiFileInfo.addBlock(blockMeta.getBlockLocation().tierAlias(), blockId, blockSize, -1);
      }
    }
    if (!blockExistOnWorker) {
      throw new FileDoesNotExistException(filePath.toString());
    }
    return uiFileInfo;
  }
}
