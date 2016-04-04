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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.StorageTierAssoc;
import alluxio.Version;
import alluxio.master.AlluxioMaster;
import alluxio.master.MasterContext;
import alluxio.underfs.UnderFileSystem;
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

  private final transient AlluxioMaster mMaster;
  private final transient Configuration mConfiguration;

  /**
   * Creates a new instance of {@link WebInterfaceGeneralServlet}.
   *
   * @param master Alluxio master
   */
  public WebInterfaceGeneralServlet(AlluxioMaster master) {
    mMaster = master;
    mConfiguration = MasterContext.getConf();
  }

  /**
   * Redirects the request to a JSP after populating attributes via populateValues.
   *
   * @param request the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   * @throws ServletException if the target resource throws this exception
   * @throws IOException if the target resource throws this exception
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
    StorageTierAssoc globalStorageTierAssoc = mMaster.getBlockMaster().getGlobalStorageTierAssoc();
    List<StorageTierInfo> infos = new ArrayList<StorageTierInfo>();
    Map<String, Long> totalBytesOnTiers = mMaster.getBlockMaster().getTotalBytesOnTiers();
    Map<String, Long> usedBytesOnTiers = mMaster.getBlockMaster().getUsedBytesOnTiers();

    for (int ordinal = 0; ordinal < globalStorageTierAssoc.size(); ordinal++) {
      String tierAlias = globalStorageTierAssoc.getAlias(ordinal);
      if (totalBytesOnTiers.containsKey(tierAlias) && totalBytesOnTiers.get(tierAlias) > 0) {
        StorageTierInfo info =
            new StorageTierInfo(tierAlias, totalBytesOnTiers.get(tierAlias),
                usedBytesOnTiers.get(tierAlias));
        infos.add(info);
      }
    }
    StorageTierInfo[] ret = infos.toArray(new StorageTierInfo[infos.size()]);

    return ret;
  }

  /**
   * Populates key, value pairs for UI display.
   *
   * @param request The {@link HttpServletRequest} object
   * @throws IOException if an I/O error occurs
   */
  private void populateValues(HttpServletRequest request) throws IOException {
    request.setAttribute("debug", mConfiguration.getBoolean(Constants.DEBUG));

    request.setAttribute("masterNodeAddress", mMaster.getMasterAddress().toString());

    request.setAttribute("uptime",
        WebUtils.convertMsToClockTime(System.currentTimeMillis() - mMaster.getStarttimeMs()));

    request.setAttribute("startTime", WebUtils.convertMsToDate(mMaster.getStarttimeMs()));

    request.setAttribute("version", Version.VERSION);

    request.setAttribute("liveWorkerNodes",
        Integer.toString(mMaster.getBlockMaster().getWorkerCount()));

    request.setAttribute("capacity",
        FormatUtils.getSizeFromBytes(mMaster.getBlockMaster().getCapacityBytes()));

    request.setAttribute("usedCapacity",
        FormatUtils.getSizeFromBytes(mMaster.getBlockMaster().getUsedBytes()));

    request
        .setAttribute("freeCapacity",
            FormatUtils.getSizeFromBytes(mMaster.getBlockMaster().getCapacityBytes()
                - mMaster.getBlockMaster().getUsedBytes()));

    // TODO(jiri): Should we use MasterContext here instead?
    Configuration conf = new Configuration();
    String ufsRoot = conf.get(Constants.UNDERFS_ADDRESS);
    UnderFileSystem ufs = UnderFileSystem.get(ufsRoot, conf);

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
