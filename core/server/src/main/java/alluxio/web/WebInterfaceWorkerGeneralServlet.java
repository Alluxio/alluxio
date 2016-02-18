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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.Lists;

import alluxio.Constants;
import alluxio.Version;
import alluxio.collections.Pair;
import alluxio.util.FormatUtils;
import alluxio.worker.WorkerContext;
import alluxio.worker.block.BlockStoreMeta;
import alluxio.worker.block.BlockWorker;

/**
 * Servlets that shows a worker's general information, including tiered storage details.
 */
@ThreadSafe
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
    public static final boolean DEBUG = WorkerContext.getConf().getBoolean(Constants.DEBUG);
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
      return WebUtils.convertMsToDate(mStartTimeMs);
    }

    /**
     * @return the uptime
     */
    public String getUptime() {
      return WebUtils.convertMsToClockTime(System.currentTimeMillis() - mStartTimeMs);
    }

    /**
     * @return the worker address
     */
    public String getWorkerAddress() {
      return mWorkerAddress;
    }

  }

  /**
   * A wrapper class of the usage info per tier for displaying in the UI.
   * This is mainly used to avoid using Map in jsp, which could cause problem with Java 8.
   */
  public static class UIUsageOnTier {
    private final String mTierAlias;
    private final long mCapacityBytes;
    private final long mUsedBytes;

    /**
     * Creates a new instance of {@link UIUsageOnTier}.
     *
     * @param tierAlias tier alias
     * @param capacityBytes capacity in bytes
     * @param usedBytes used space in bytes
     */
    public UIUsageOnTier(String tierAlias, long capacityBytes, long usedBytes) {
      mTierAlias = tierAlias;
      mCapacityBytes = capacityBytes;
      mUsedBytes = usedBytes;
    }

    /**
     * @return the tier alias
     */
    public String getTierAlias() {
      return mTierAlias;
    }

    /**
     * @return capacity in bytes
     */
    public long getCapacityBytes() {
      return mCapacityBytes;
    }

    /**
     * @return used space in bytes
     */
    public long getUsedBytes() {
      return mUsedBytes;
    }

  }

  private static final long serialVersionUID = 3735143768058466487L;
  private final transient BlockWorker mBlockWorker;
  private final UIWorkerInfo mUiWorkerInfo;

  /**
   * Creates a new instance of {@link WebInterfaceWorkerGeneralServlet}.
   *
   * @param blockWorker block worker handle
   * @param workerAddress  worker address
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
    List<UIUsageOnTier> usageOnTiers = Lists.newArrayList();
    for (Entry<String, Long> entry : capacityBytesOnTiers.entrySet()) {
      String tier = entry.getKey();
      long capacity = entry.getValue();
      Long nullableUsed = usedBytesOnTiers.get(tier);
      long used = nullableUsed == null ? 0 : nullableUsed;

      capacityBytes += capacity;
      usedBytes += used;

      usageOnTiers.add(new UIUsageOnTier(tier, capacity, used));
    }

    request.setAttribute("capacityBytes", FormatUtils.getSizeFromBytes(capacityBytes));

    request.setAttribute("usedBytes", FormatUtils.getSizeFromBytes(usedBytes));

    request.setAttribute("usageOnTiers", usageOnTiers);

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
