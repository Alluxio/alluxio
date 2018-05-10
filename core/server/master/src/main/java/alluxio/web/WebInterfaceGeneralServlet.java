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
import alluxio.master.meta.checkconf.WrongProperty;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
  private static final Logger LOG = LoggerFactory.getLogger(WebInterfaceGeneralServlet.class);

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

  /**
   * Class to make wrong property information more intuitive and human-readable.
   */
  public static final class WrongPropertyInfo {
    /** The name of the property that has errors/warnings.*/
    private String mName;
    /**
     * Record the values and corresponding hostnames.
     * Each string in valuesAndHosts is of format Value (Host1, Host2, ...)
     */
    private List<String> mValuesAndHosts;

    /**
     * Creates a new instance of {@link WrongProperty}.
     */
    private WrongPropertyInfo() {}

    /**
     * @return the name of this property
     */
    public String getName() {
      return mName;
    }

    /**
     * @return the values and hostnames of this property
     */
    public List<String> getValuesAndHosts() {
      return mValuesAndHosts;
    }

    /**
     * @param name the property name
     * @return the wrong property
     */
    private WrongPropertyInfo setName(String name) {
      mName = name;
      return this;
    }

    /**
     * @param valuesAndHosts the values to use
     * @return the wrong property
     */
    private WrongPropertyInfo setValuesAndHosts(List<String> valuesAndHosts) {
      mValuesAndHosts = valuesAndHosts;
      return this;
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
   * Generates the wrong property info from wrong property for human-readable.
   *
   * @param wrongProperty the wrong property to transfrom
   * @return generated wrong property info
   */
  private WrongPropertyInfo generateWrongPropertyInfo(WrongProperty wrongProperty) {
    // Each String in valuesAndHosts is of format Value (Host1, Host2, ...)
    String valueAndHostsFormat = "%s (%s)";
    List<String> valuesAndHosts = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : wrongProperty.getValues().entrySet()) {
      valuesAndHosts.add(String.format(valueAndHostsFormat, entry.getKey(),
          String.join(",", entry.getValue())));
    }
    return new WrongPropertyInfo()
        .setName(wrongProperty.getName()).setValuesAndHosts(valuesAndHosts);
  }

  /**
   * Populates key, value pairs for UI display.
   *
   * @param request The {@link HttpServletRequest} object
   */
  private void populateValues(HttpServletRequest request) {
    BlockMaster blockMaster = mMasterProcess.getMaster(BlockMaster.class);
    FileSystemMaster fileSystemMaster = mMasterProcess.getMaster(FileSystemMaster.class);

    request.setAttribute("debug", Configuration.getBoolean(PropertyKey.DEBUG));

    request.setAttribute("masterNodeAddress", mMasterProcess.getRpcAddress().toString());

    request.setAttribute("uptime", CommonUtils
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

    request.setAttribute("configCheckStatus", mMasterProcess.getConfStatus());
    List<WrongPropertyInfo> confErrors = mMasterProcess.getConfErrors().stream()
        .map(this::generateWrongPropertyInfo).collect(Collectors.toList());
    List<WrongPropertyInfo> confWarns = mMasterProcess.getConfWarns().stream()
        .map(this::generateWrongPropertyInfo).collect(Collectors.toList());
    request.setAttribute("inconsistentProperties", confErrors.size() + confWarns.size());
    request.setAttribute("confErrorsItem", confErrors);
    request.setAttribute("confErrorsNum", confErrors.size());
    request.setAttribute("confWarnsItem", confWarns);
    request.setAttribute("confWarnsNum", confWarns.size());

    String ufsRoot = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsRoot);

    String totalSpace = "UNKNOWN";
    try {
      long sizeBytes = ufs.getSpace(ufsRoot, UnderFileSystem.SpaceType.SPACE_TOTAL);
      if (sizeBytes >= 0) {
        totalSpace = FormatUtils.getSizeFromBytes(sizeBytes);
      }
    } catch (IOException e) {
      // Exception may be thrown when UFS connection is lost
      LOG.warn("Failed to get size of total space of root UFS: {}", e.getMessage());
    }
    request.setAttribute("diskCapacity", totalSpace);

    String usedSpace = "UNKNOWN";
    try {
      long sizeBytes = ufs.getSpace(ufsRoot, UnderFileSystem.SpaceType.SPACE_USED);
      if (sizeBytes >= 0) {
        usedSpace = FormatUtils.getSizeFromBytes(sizeBytes);
      }
    } catch (IOException e) {
      // Exception may be thrown when UFS connection is lost
      LOG.warn("Failed to get size of used space of root UFS: {}", e.getMessage());
    }
    request.setAttribute("diskUsedCapacity", usedSpace);

    String freeSpace = "UNKNOWN";
    try {
      long sizeBytes = ufs.getSpace(ufsRoot, UnderFileSystem.SpaceType.SPACE_FREE);
      if (sizeBytes >= 0) {
        freeSpace = FormatUtils.getSizeFromBytes(sizeBytes);
      }
    } catch (IOException e) {
      // Exception may be thrown when UFS connection is lost
      LOG.warn("Failed to get size of free space of root UFS: {}", e.getMessage());
    }
    request.setAttribute("diskFreeCapacity", freeSpace);

    StorageTierInfo[] infos = generateOrderedStorageTierInfo();
    request.setAttribute("storageTierInfos", infos);
  }
}
