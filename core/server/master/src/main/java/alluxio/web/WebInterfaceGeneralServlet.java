/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.web;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.StorageTierAssoc;
import alluxio.master.MasterProcess;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.StartupConsistencyCheck;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet that provides data for viewing the general status of the filesystem.
 */
@ThreadSafe
public final class WebInterfaceGeneralServlet extends HttpServlet {
  /**
   * Class to make referencing tiered storage information more intuitive.
   */
  public static final class StorageTierInfo {
    private final String mStorageTierAlias;
    private final long mCapacityBytes;
    private final long mUsedBytes;
    private final int mUsedPercent;
    private final long mFreeBytes;
    private final int mFreePercent;

    private StorageTierInfo(String storageTierAlias, long capacityBytes, long usedBytes) {
      mStorageTierAlias = storageTierAlias;
      mCapacityBytes = capacityBytes;
      mUsedBytes = usedBytes;
      mFreeBytes = mCapacityBytes - mUsedBytes;
      mUsedPercent = (int) (100L * mUsedBytes / mCapacityBytes);
      mFreePercent = 100 - mUsedPercent;
    }

    /**
     * @return the storage alias
     */
    public String getStorageTierAlias() {
      return mStorageTierAlias;
    }

    /**
     * @return the capacity
     */
    public String getCapacity() {
      return FormatUtils.getSizeFromBytes(mCapacityBytes);
    }

    /**
     * @return the free capacity
     */
    public String getFreeCapacity() {
      return FormatUtils.getSizeFromBytes(mFreeBytes);
    }

    /**
     * @return the free space as a percentage
     */
    public int getFreeSpacePercent() {
      return mFreePercent;
    }

    /**
     * @return the used capacity
     */
    public String getUsedCapacity() {
      return FormatUtils.getSizeFromBytes(mUsedBytes);
    }

    /**
     * @return the used space as a percentage
     */
    public int getUsedSpacePercent() {
      return mUsedPercent;
    }
  }

  private static final long serialVersionUID = 2335205655766736309L;

  private final transient MasterProcess mMasterProcess;

  /**
   * Creates a new instance of {@link WebInterfaceGeneralServlet}.
   *
   * @param masterProcess Alluxio master process
   */
  public WebInterfaceGeneralServlet(MasterProcess masterProcess) {
    mMasterProcess = masterProcess;
  }

  /**
   * Redirects the request to a JSP after populating attributes via populateValues.
   *
   * @param request the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   * @throws ServletException if the target resource throws this exception
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    populateValues(request);
    getServletContext().getRequestDispatcher("/general.jsp").forward(request, response);
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    populateValues(request);
    getServletContext().getRequestDispatcher("/general.jsp").forward(request, response);
  }

  /**
   * Lists the {@link StorageTierInfo} objects of each storage level alias.
   *
   * @return the list of {@link StorageTierInfo} objects, in order from highest tier to lowest
   */
  private StorageTierInfo[] generateOrderedStorageTierInfo() {
    BlockMaster blockMaster = mMasterProcess.getMaster(BlockMaster.class);
    StorageTierAssoc globalStorageTierAssoc = blockMaster.getGlobalStorageTierAssoc();
    List<StorageTierInfo> infos = new ArrayList<>();
    Map<String, Long> totalBytesOnTiers = blockMaster.getTotalBytesOnTiers();
    Map<String, Long> usedBytesOnTiers = blockMaster.getUsedBytesOnTiers();

    for (int ordinal = 0; ordinal < globalStorageTierAssoc.size(); ordinal++) {
      String tierAlias = globalStorageTierAssoc.getAlias(ordinal);
      if (totalBytesOnTiers.containsKey(tierAlias) && totalBytesOnTiers.get(tierAlias) > 0) {
        StorageTierInfo info =
            new StorageTierInfo(tierAlias, totalBytesOnTiers.get(tierAlias),
                usedBytesOnTiers.get(tierAlias));
        infos.add(info);
      }
    }

    return infos.toArray(new StorageTierInfo[infos.size()]);
  }

  /**
   * Populates key, value pairs for UI display.
   *
   * @param request The {@link HttpServletRequest} object
   */
  private void populateValues(HttpServletRequest request) throws IOException {
    BlockMaster blockMaster = mMasterProcess.getMaster(BlockMaster.class);
    FileSystemMaster fileSystemMaster = mMasterProcess.getMaster(FileSystemMaster.class);

    request.setAttribute("debug", Configuration.getBoolean(PropertyKey.DEBUG));

    request.setAttribute("masterNodeAddress", mMasterProcess.getRpcAddress().toString());

    request.setAttribute("uptime", WebUtils
        .convertMsToClockTime(System.currentTimeMillis() - mMasterProcess.getStartTimeMs()));

    request.setAttribute("startTime", CommonUtils.convertMsToDate(mMasterProcess.getStartTimeMs()));

    request.setAttribute("version", RuntimeConstants.VERSION);

    request.setAttribute("liveWorkerNodes",
        Integer.toString(blockMaster.getWorkerCount()));

    request.setAttribute("capacity",
        FormatUtils.getSizeFromBytes(blockMaster.getCapacityBytes()));

    request.setAttribute("usedCapacity",
        FormatUtils.getSizeFromBytes(blockMaster.getUsedBytes()));

    request.setAttribute("freeCapacity",
        FormatUtils.getSizeFromBytes(blockMaster.getCapacityBytes() - blockMaster.getUsedBytes()));

    StartupConsistencyCheck check = fileSystemMaster.getStartupConsistencyCheck();
    request.setAttribute("consistencyCheckStatus", check.getStatus());
    if (check.getStatus() == StartupConsistencyCheck.Status.COMPLETE) {
      request.setAttribute("inconsistentPaths", check.getInconsistentUris().size());
      request.setAttribute("inconsistentPathItems", check.getInconsistentUris());
    } else {
      request.setAttribute("inconsistentPaths", 0);
    }

    String ufsRoot = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsRoot);

    long sizeBytes = ufs.getSpace(ufsRoot, UnderFileSystem.SpaceType.SPACE_TOTAL);
    if (sizeBytes >= 0) {
      request.setAttribute("diskCapacity", FormatUtils.getSizeFromBytes(sizeBytes));
    } else {
      request.setAttribute("diskCapacity", "UNKNOWN");
    }

    sizeBytes = ufs.getSpace(ufsRoot, UnderFileSystem.SpaceType.SPACE_USED);
    if (sizeBytes >= 0) {
      request.setAttribute("diskUsedCapacity", FormatUtils.getSizeFromBytes(sizeBytes));
    } else {
      request.setAttribute("diskUsedCapacity", "UNKNOWN");
    }

    sizeBytes = ufs.getSpace(ufsRoot, UnderFileSystem.SpaceType.SPACE_FREE);
    if (sizeBytes >= 0) {
      request.setAttribute("diskFreeCapacity", FormatUtils.getSizeFromBytes(sizeBytes));
    } else {
      request.setAttribute("diskFreeCapacity", "UNKNOWN");
    }

    StorageTierInfo[] infos = generateOrderedStorageTierInfo();
    request.setAttribute("storageTierInfos", infos);
  }
}
