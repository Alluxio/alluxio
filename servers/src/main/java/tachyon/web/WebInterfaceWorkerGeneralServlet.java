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
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import tachyon.Constants;
import tachyon.Version;
import tachyon.util.CommonUtils;
import tachyon.worker.block.BlockStoreMeta;
import tachyon.worker.block.BlockWorker;

/**
 * Servlets that shows a worker's general information, including tiered storage details.
 */
public class WebInterfaceWorkerGeneralServlet extends HttpServlet {

  public static class UiStorageDir {
    private final long mStorageDirId;
    private final String mDirPath;
    private final long mCapacityBytes;
    private final long mUsedBytes;

    public UiStorageDir(long storageDirId, String dirPath, long capacityBytes, long usedBytes) {
      mStorageDirId = storageDirId;
      mDirPath = dirPath;
      mCapacityBytes = capacityBytes;
      mUsedBytes = usedBytes;
    }

    public long getCapacityBytes() {
      return mCapacityBytes;
    }

    public String getDirPath() {
      return mDirPath;
    }

    public long getStorageDirId() {
      return mStorageDirId;
    }

    public long getUsedBytes() {
      return mUsedBytes;
    }
  }

  private static final long serialVersionUID = 3735143768058466487L;
  private final transient BlockWorker mWorker;

  public WebInterfaceWorkerGeneralServlet(BlockWorker worker) {
    mWorker = worker;
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    populateValues(request);
    getServletContext().getRequestDispatcher("/worker/general.jsp").forward(request, response);
  }

  /**
   * Populates key, value pairs for UI display
   *
   * @param request The HttpServletRequest object
   * @throws IOException
   */
  private void populateValues(HttpServletRequest request) throws IOException {
    request.setAttribute("debug", Constants.DEBUG);

    request.setAttribute("workerAddress", mWorker.getWorkerNetAddress().getMHost() + ":"
        + mWorker.getWorkerNetAddress().getMPort());

    request.setAttribute("uptime",
        Utils.convertMsToClockTime(System.currentTimeMillis() - mWorker.getStartTimeMs()));

    request.setAttribute("startTime", Utils.convertMsToDate(mWorker.getStartTimeMs()));

    request.setAttribute("version", Version.VERSION);

    BlockStoreMeta storeMeta = mWorker.getStoreMeta();
    long capacityBytes = 0L;
    long usedBytes = 0L;
    List<Long> capacityBytesOnTiers = storeMeta.getCapacityBytesOnTiers();
    List<Long> usedBytesOnTiers = storeMeta.getUsedBytesOnTiers();
    for (int i = 0; i < capacityBytesOnTiers.size(); i ++) {
      capacityBytes += capacityBytesOnTiers.get(i);
      usedBytes += usedBytesOnTiers.get(i);
    }

    request.setAttribute("capacityBytes", CommonUtils.getSizeFromBytes(capacityBytes));

    request.setAttribute("usedBytes", CommonUtils.getSizeFromBytes(usedBytes));

    request.setAttribute("capacityBytesOnTiers", capacityBytesOnTiers);

    request.setAttribute("usedBytesOnTiers", usedBytesOnTiers);

    List<UiStorageDir> storageDirs = new ArrayList<UiStorageDir>(storeMeta.getDirPaths().size());
    for (long dirId : storeMeta.getDirPaths().keySet()) {
      storageDirs.add(new UiStorageDir(dirId, storeMeta.getDirPaths().get(dirId), storeMeta
          .getCapacityBytesOnDirs().get(dirId), storeMeta.getUsedBytesOnDirs().get(dirId)));
    }

    request.setAttribute("storageDirs", storageDirs);
  }
}
