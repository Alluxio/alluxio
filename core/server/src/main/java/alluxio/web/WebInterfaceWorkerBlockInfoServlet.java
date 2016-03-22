/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.web;

import alluxio.AlluxioURI;
import alluxio.WorkerStorageTierAssoc;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.block.BlockId;
import alluxio.worker.WorkerContext;
import alluxio.worker.block.BlockStoreMeta;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.meta.BlockMeta;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet that provides data for displaying block info of a worker.
 */
@ThreadSafe
public final class WebInterfaceWorkerBlockInfoServlet extends HttpServlet {
  private static final long serialVersionUID = 4148506607369321012L;
  private final transient BlockWorker mBlockWorker;

  /**
   * Creates a new instance of {@link WebInterfaceWorkerBlockInfoServlet}.
   *
   * @param blockWorker block worker handle
   */
  public WebInterfaceWorkerBlockInfoServlet(BlockWorker blockWorker) {
    mBlockWorker = Preconditions.checkNotNull(blockWorker);
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
    FileSystem fs = FileSystem.Factory.get();
    String filePath = request.getParameter("path");
    if (!(filePath == null || filePath.isEmpty())) {
      // Display file block info
      try {
        UIFileInfo uiFileInfo = getUiFileInfo(fs, new AlluxioURI(filePath));
        List<ImmutablePair<String, List<UIFileBlockInfo>>> fileBlocksOnTier = Lists.newArrayList();
        for (Entry<String, List<UIFileBlockInfo>> e : uiFileInfo.getBlocksOnTier().entrySet()) {
          fileBlocksOnTier.add(
              new ImmutablePair<String, List<UIFileBlockInfo>>(e.getKey(), e.getValue()));
        }
        request.setAttribute("fileBlocksOnTier", fileBlocksOnTier);
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
      } catch (AlluxioException e) {
        request.setAttribute("fatalError", "Error: alluxio exception. " + e.getMessage());
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
        uiFileInfos.add(getUiFileInfo(fs, fileId));
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
    } catch (AlluxioException e) {
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
    BlockStoreMeta storeMeta = mBlockWorker.getStoreMeta();
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
   * @throws BlockDoesNotExistException if the block does not exist
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an unexpected Alluxio exception is thrown
   */
  private UIFileInfo getUiFileInfo(FileSystem fileSystem, long fileId)
      throws FileDoesNotExistException, BlockDoesNotExistException, IOException, AlluxioException {
    // TODO(calvin): Remove this dependency
    FileSystemMasterClient masterClient = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      return getUiFileInfo(fileSystem, new AlluxioURI(masterClient.getStatusInternal(fileId)
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
   * @throws BlockDoesNotExistException if the block does not exist
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the path is invalid
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an unexpected Alluxio exception is thrown
   */
  private UIFileInfo getUiFileInfo(FileSystem fileSystem, AlluxioURI filePath)
      throws BlockDoesNotExistException, FileDoesNotExistException, InvalidPathException,
      IOException, AlluxioException {
    URIStatus status;
    try {
      status = fileSystem.getStatus(filePath);
    } catch (AlluxioException e) {
      throw new FileDoesNotExistException(filePath.toString());
    }
    UIFileInfo uiFileInfo = new UIFileInfo(status);
    boolean blockExistOnWorker = false;
    for (long blockId : status.getBlockIds()) {
      if (mBlockWorker.hasBlockMeta(blockId)) {
        blockExistOnWorker = true;
        BlockMeta blockMeta = mBlockWorker.getVolatileBlockMeta(blockId);
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
