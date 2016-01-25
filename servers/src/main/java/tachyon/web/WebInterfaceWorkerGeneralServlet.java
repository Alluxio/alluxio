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
import tachyon.worker.block.BlockStoreMeta;
import tachyon.worker.block.BlockWorker;

/**
 * Servlets that shows a worker's general information, including tiered storage details.
 */
public final class WebInterfaceWorkerGeneralServlet extends HttpServlet {

  /**
   * Displays information about a storage directory in the UI.
   */
  public static class UIStorageDir {
    private final String mTierAlias;
    private final String mDirPath;
    private final long mCapacityBytes;
    private final long mUsedBytes;

    /**
     * Creates a new instance of {@link UIStorageDir}.
     *
     * @param tierAlias tier alias
     * @param dirPath directory path
     * @param capacityBytes capacity in bytes
     * @param usedBytes used capacity in bytes
     */
    public UIStorageDir(String tierAlias, String dirPath, long capacityBytes, long usedBytes) {
      mTierAlias = tierAlias;
      mDirPath = dirPath;
      mCapacityBytes = capacityBytes;
      mUsedBytes = usedBytes;
    }

    /**
     * @return capacity in bytes
     */
    public long getCapacityBytes() {
      return mCapacityBytes;
    }

    /**
     * @return directory path
     */
    public String getDirPath() {
      return mDirPath;
    }

    /**
     * @return tier alias
     */
    public String getTierAlias() {
      return mTierAlias;
    }

    /**
     * @return used capacity in bytes
     */
    public long getUsedBytes() {
      return mUsedBytes;
    }
  }

  /**
   * Displays information about a worker in the UI.
   */
  public static class UIWorkerInfo {
    public static final boolean DEBUG = Constants.DEBUG;
    public static final String VERSION = Version.VERSION;
    private final String mWorkerAddress;
    private final long mStartTimeMs;

    /**
     * Creates a new instance of {@link UIWorkerInfo}.
     *
     * @param workerAddress worker address
     * @param startTimeMs start time in milliseconds
     */
    public UIWorkerInfo(String workerAddress, long startTimeMs) {
      mWorkerAddress = workerAddress;
      mStartTimeMs = startTimeMs;
    }

    /**
     * @return the start time
     */
    public String getStartTime() {
      return Utils.convertMsToDate(mStartTimeMs);
    }

    /**
     * @return the uptime
     */
    public String getUptime() {
      return Utils.convertMsToClockTime(System.currentTimeMillis() - mStartTimeMs);
    }

    /**
     * @return the worker address
     */
    public String getWorkerAddress() {
      return mWorkerAddress;
    }

  }

  private static final long serialVersionUID = 3735143768058466487L;
  private final transient BlockWorker mBlockWorker;
  private final UIWorkerInfo mUiWorkerInfo;

  /**
   * Creates a new instance of {@link WebInterfaceWorkerGeneralServlet}.
   *
   * @param blockWorker block worker handle
   * @param workerAddress worker address
   * @param startTimeMs start time in milliseconds
   */
  public WebInterfaceWorkerGeneralServlet(BlockWorker blockWorker,
      InetSocketAddress workerAddress, long startTimeMs) {
    mBlockWorker = blockWorker;
    mUiWorkerInfo = new UIWorkerInfo(workerAddress.toString(), startTimeMs);
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    populateValues(request);
    getServletContext().getRequestDispatcher("/worker/general.jsp").forward(request, response);
  }

  /**
   * Populates key, value pairs for UI display.
   *
   * @param request the {@link HttpServletRequest} object
   * @throws IOException if an I/O error occurs
   */
  private void populateValues(HttpServletRequest request) throws IOException {
    request.setAttribute("workerInfo", mUiWorkerInfo);

    BlockStoreMeta storeMeta = mBlockWorker.getStoreMeta();
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

    List<UIStorageDir> storageDirs =
        new ArrayList<UIStorageDir>(storeMeta.getCapacityBytesOnDirs().size());
    for (Pair<String, String> tierAndDirPath : storeMeta.getCapacityBytesOnDirs().keySet()) {
      storageDirs.add(new UIStorageDir(tierAndDirPath.getFirst(), tierAndDirPath.getSecond(),
          storeMeta.getCapacityBytesOnDirs().get(tierAndDirPath), storeMeta.getUsedBytesOnDirs()
              .get(tierAndDirPath)));
    }

    request.setAttribute("storageDirs", storageDirs);
  }
}
