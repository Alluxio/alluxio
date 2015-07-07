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
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import tachyon.Constants;
import tachyon.StorageLevelAlias;
import tachyon.Version;
import tachyon.master.DependencyVariables;
import tachyon.master.MasterInfo;
import tachyon.util.CommonUtils;

/**
 * Servlet that provides data for viewing the general status of the filesystem.
 */
public class WebInterfaceGeneralServlet extends HttpServlet {
  /**
   * Class to make referencing tiered storage information more intuitive.
   */
  public static class StorageTierInfo {
    private final StorageLevelAlias mStorageLevelAlias;
    private final long mCapacityBytes;
    private final long mUsedBytes;
    private final int mUsedPercent;
    private final long mFreeBytes;
    private final int mFreePercent;

    private StorageTierInfo(int storageLevelAliasValue, long capacityBytes, long usedBytes) {
      mStorageLevelAlias = StorageLevelAlias.values()[storageLevelAliasValue - 1];
      mCapacityBytes = capacityBytes;
      mUsedBytes = usedBytes;
      mFreeBytes = mCapacityBytes - mUsedBytes;
      mUsedPercent = (int) (100L * mUsedBytes / mCapacityBytes);
      mFreePercent = 100 - mUsedPercent;
    }

    public String getStorageLevelAlias() {
      return mStorageLevelAlias.name();
    }

    public String getCapacity() {
      return CommonUtils.getSizeFromBytes(mCapacityBytes);
    }

    public String getFreeCapacity() {
      return CommonUtils.getSizeFromBytes(mFreeBytes);
    }

    public int getFreeSpacePercent() {
      return mFreePercent;
    }

    public String getUsedCapacity() {
      return CommonUtils.getSizeFromBytes(mUsedBytes);
    }

    public int getUsedSpacePercent() {
      return mUsedPercent;
    }
  }

  private static final long serialVersionUID = 2335205655766736309L;

  private final transient MasterInfo mMasterInfo;

  public WebInterfaceGeneralServlet(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }

  /**
   * Redirects the request to a JSP after populating attributes via populateValues.
   * 
   * @param request The HttpServletRequest object
   * @param response The HttpServletResponse object
   * @throws ServletException
   * @throws IOException
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    populateValues(request);
    getServletContext().getRequestDispatcher("/general.jsp").forward(request, response);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    DependencyVariables.VARIABLES.clear();
    for (String key : (Set<String>) request.getParameterMap().keySet()) {
      if (key.startsWith("varName")) {
        String value = request.getParameter("varVal" + key.substring(7));
        if (value != null) {
          DependencyVariables.VARIABLES.put(request.getParameter(key), value);
        }
      }
    }
    populateValues(request);
    getServletContext().getRequestDispatcher("/general.jsp").forward(request, response);
  }

  /**
   * List the StorageTierInfo objects of each storage level(alias).
   * 
   * @return the list of StorageTierInfo objects.
   */
  private StorageTierInfo[] generateOrderedStorageTierInfo() {
    List<StorageTierInfo> infos = new ArrayList<StorageTierInfo>();
    List<Long> totalBytesOnTiers = mMasterInfo.getTotalBytesOnTiers();
    List<Long> usedBytesOnTiers = mMasterInfo.getUsedBytesOnTiers();

    for (int i = 0; i < totalBytesOnTiers.size(); i ++) {
      if (totalBytesOnTiers.get(i) > 0) {
        StorageTierInfo info =
            new StorageTierInfo(i + 1, totalBytesOnTiers.get(i), usedBytesOnTiers.get(i));
        infos.add(info);
      }
    }
    StorageTierInfo[] ret = infos.toArray(new StorageTierInfo[infos.size()]);

    return ret;
  }

  /**
   * Populates key, value pairs for UI display
   * 
   * @param request The HttpServletRequest object
   * @throws IOException
   */
  private void populateValues(HttpServletRequest request) throws IOException {
    request.setAttribute("debug", Constants.DEBUG);

    request.setAttribute("masterNodeAddress", mMasterInfo.getMasterAddress().toString());

    request.setAttribute("uptime",
        Utils.convertMsToClockTime(System.currentTimeMillis() - mMasterInfo.getStarttimeMs()));

    request.setAttribute("startTime", Utils.convertMsToDate(mMasterInfo.getStarttimeMs()));

    request.setAttribute("version", Version.VERSION);

    request.setAttribute("liveWorkerNodes", Integer.toString(mMasterInfo.getWorkerCount()));

    request.setAttribute("capacity", CommonUtils.getSizeFromBytes(mMasterInfo.getCapacityBytes()));

    request.setAttribute("usedCapacity", CommonUtils.getSizeFromBytes(mMasterInfo.getUsedBytes()));

    request
        .setAttribute("freeCapacity",
            CommonUtils.getSizeFromBytes((mMasterInfo.getCapacityBytes() - mMasterInfo
                .getUsedBytes())));

    long sizeBytes = mMasterInfo.getUnderFsCapacityBytes();
    if (sizeBytes >= 0) {
      request.setAttribute("diskCapacity", CommonUtils.getSizeFromBytes(sizeBytes));
    } else {
      request.setAttribute("diskCapacity", "UNKNOWN");
    }

    sizeBytes = mMasterInfo.getUnderFsUsedBytes();
    if (sizeBytes >= 0) {
      request.setAttribute("diskUsedCapacity", CommonUtils.getSizeFromBytes(sizeBytes));
    } else {
      request.setAttribute("diskUsedCapacity", "UNKNOWN");
    }

    sizeBytes = mMasterInfo.getUnderFsFreeBytes();
    if (sizeBytes >= 0) {
      request.setAttribute("diskFreeCapacity", CommonUtils.getSizeFromBytes(sizeBytes));
    } else {
      request.setAttribute("diskFreeCapacity", "UNKNOWN");
    }

    request.setAttribute("recomputeVariables", DependencyVariables.VARIABLES);

    StorageTierInfo[] infos = generateOrderedStorageTierInfo();
    request.setAttribute("storageTierInfos", infos);
  }
}
