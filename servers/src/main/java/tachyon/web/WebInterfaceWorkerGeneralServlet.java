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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import tachyon.Constants;
import tachyon.Version;
import tachyon.collections.Pair;
import tachyon.util.FormatUtils;
import tachyon.worker.block.BlockDataManager;
import tachyon.worker.block.BlockStoreMeta;

/**
 * Servlets that shows a worker's general information, including tiered storage details.
 */
public final class WebInterfaceWorkerGeneralServlet extends HttpServlet {

  public static class UiStorageDir {
    private final String mTierAlias;
    private final String mDirPath;
    private final long mCapacityBytes;
    private final long mUsedBytes;

    public UiStorageDir(String tierAlias, String dirPath, long capacityBytes, long usedBytes) {
      mTierAlias = tierAlias;
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

    public String getTierAlias() {
      return mTierAlias;
    }

    public long getUsedBytes() {
      return mUsedBytes;
    }
  }

  public static class UiWorkerInfo {
    public static final boolean DEBUG = Constants.DEBUG;
    public static final String VERSION = Version.VERSION;
    private final String mWorkerAddress;
    private final long mStartTimeMs;

    public UiWorkerInfo(String workerAddress, long startTimeMs) {
      mWorkerAddress = workerAddress;
      mStartTimeMs = startTimeMs;
    }

    public String getStartTime() {
      return Utils.convertMsToDate(mStartTimeMs);
    }

    public String getUptime() {
      return Utils.convertMsToClockTime(System.currentTimeMillis() - mStartTimeMs);
    }

    public String getWorkerAddress() {
      return mWorkerAddress;
    }

  }

  private static final long serialVersionUID = 3735143768058466487L;
  private final transient BlockDataManager mBlockDataManager;
  private final UiWorkerInfo mUiWorkerInfo;

  public WebInterfaceWorkerGeneralServlet(BlockDataManager blockDataManager,
      InetSocketAddress workerAddress, long startTimeMs) {
    mBlockDataManager = blockDataManager;
    mUiWorkerInfo = new UiWorkerInfo(workerAddress.toString(), startTimeMs);
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
    request.setAttribute("workerInfo", mUiWorkerInfo);

    BlockStoreMeta storeMeta = mBlockDataManager.getStoreMeta();
    long capacityBytes = 0L;
    long usedBytes = 0L;
    Map<String, Long> capacityBytesOnTiers = storeMeta.getCapacityBytesOnTiers();
    Map<String, Long> usedBytesOnTiers = storeMeta.getUsedBytesOnTiers();
    for (long capacity : capacityBytesOnTiers.values()) {
      capacityBytes += capacity;
    }
    for (long used : usedBytesOnTiers.values()) {
      usedBytes += used;
    }

    request.setAttribute("capacityBytes", FormatUtils.getSizeFromBytes(capacityBytes));

    request.setAttribute("usedBytes", FormatUtils.getSizeFromBytes(usedBytes));

    request.setAttribute("capacityBytesOnTiers", capacityBytesOnTiers);

    request.setAttribute("usedBytesOnTiers", usedBytesOnTiers);

    List<UiStorageDir> storageDirs =
        new ArrayList<UiStorageDir>(storeMeta.getCapacityBytesOnDirs().size());
    for (Pair<String, String> tierAndDirPath : storeMeta.getCapacityBytesOnDirs().keySet()) {
      storageDirs.add(new UiStorageDir(tierAndDirPath.getFirst(), tierAndDirPath.getSecond(),
          storeMeta.getCapacityBytesOnDirs().get(tierAndDirPath), storeMeta.getUsedBytesOnDirs()
              .get(tierAndDirPath)));
    }

    request.setAttribute("storageDirs", storageDirs);
  }
}
