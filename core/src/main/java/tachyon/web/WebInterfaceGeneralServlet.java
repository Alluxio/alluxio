/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.web;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import tachyon.Constants;
import tachyon.Version;
import tachyon.master.DependencyVariables;
import tachyon.master.MasterInfo;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.util.CommonUtils;

/**
 * Servlet that provides data for viewing the general status of the filesystem.
 */
public class WebInterfaceGeneralServlet extends HttpServlet {
  /**
   * Class to make referencing worker nodes more intuitive. Mainly to avoid implicit association
   * by array indexes.
   */
  public class NodeInfo {
    private final String NAME;
    private final String LAST_CONTACT_SEC;
    private final String STATE;
    private final int FREE_SPACE_PERCENT;
    private final int USED_SPACE_PERCENT;
    private final String UPTIME_CLOCK_TIME;

    private NodeInfo(ClientWorkerInfo workerInfo) {
      NAME = workerInfo.getAddress().getMHost();
      LAST_CONTACT_SEC = Integer.toString(workerInfo.getLastContactSec());
      STATE = workerInfo.getState();
      USED_SPACE_PERCENT =
          (int) (100L * workerInfo.getUsedBytes() / workerInfo.getCapacityBytes());
      FREE_SPACE_PERCENT = 100 - USED_SPACE_PERCENT;
      UPTIME_CLOCK_TIME =
          CommonUtils.convertMsToShortClockTime(System.currentTimeMillis()
              - workerInfo.getStarttimeMs());
    }

    public int getFreeSpacePercent() {
      return FREE_SPACE_PERCENT;
    }

    public String getLastHeartbeat() {
      return LAST_CONTACT_SEC;
    }

    public String getName() {
      return NAME;
    }

    public String getState() {
      return STATE;
    }

    public String getUptimeClockTime() {
      return UPTIME_CLOCK_TIME;
    }

    public int getUsedSpacePercent() {
      return USED_SPACE_PERCENT;
    }
  }

  private static final long serialVersionUID = 2335205655766736309L;

  private MasterInfo mMasterInfo;

  public WebInterfaceGeneralServlet(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }

  /**
   * Redirects the request to a JSP after populating attributes via populateValues.
   * 
   * @param request
   *          The HttpServletRequest object
   * @param response
   *          The HttpServletResponse object
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
    DependencyVariables.sVariables.clear();
    for (String key : (Set<String>) request.getParameterMap().keySet()) {
      if (key.startsWith("varName")) {
        String value = request.getParameter("varVal" + key.substring(7));
        if (value != null) {
          DependencyVariables.sVariables.put(request.getParameter(key), value);
        }
      }
    }
    populateValues(request);
    getServletContext().getRequestDispatcher("/general.jsp").forward(request, response);
  }

  /**
   * Populates key, value pairs for UI display
   * 
   * @param request
   *          The HttpServletRequest object
   * @throws IOException
   */
  private void populateValues(HttpServletRequest request) throws IOException {
    request.setAttribute("debug", Constants.DEBUG);

    request.setAttribute("masterNodeAddress", mMasterInfo.getMasterAddress().toString());

    request.setAttribute("uptime", CommonUtils.convertMsToClockTime(System.currentTimeMillis()
        - mMasterInfo.getStarttimeMs()));

    request.setAttribute("startTime", CommonUtils.convertMsToDate(mMasterInfo.getStarttimeMs()));

    request.setAttribute("version", Version.VERSION);

    request.setAttribute("liveWorkerNodes", Integer.toString(mMasterInfo.getWorkerCount()));

    request.setAttribute("capacity", CommonUtils.getSizeFromBytes(mMasterInfo.getCapacityBytes()));

    request.setAttribute("usedCapacity", CommonUtils.getSizeFromBytes(mMasterInfo.getUsedBytes()));

    request.setAttribute("freeCapacity", CommonUtils.getSizeFromBytes((mMasterInfo
        .getCapacityBytes() - mMasterInfo.getUsedBytes())));

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

    request.setAttribute("recomputeVariables", DependencyVariables.sVariables);

    List<ClientWorkerInfo> workerInfos = mMasterInfo.getWorkersInfo();
    for (int i = 0; i < workerInfos.size(); i ++) {
      for (int j = i + 1; j < workerInfos.size(); j ++) {
        if (workerInfos.get(i).getAddress().getMHost()
            .compareTo(workerInfos.get(j).getAddress().getMHost()) > 0) {
          ClientWorkerInfo temp = workerInfos.get(i);
          workerInfos.set(i, workerInfos.get(j));
          workerInfos.set(j, temp);
        }
      }
    }
    int index = 0;
    NodeInfo[] nodeInfos = new NodeInfo[workerInfos.size()];
    for (ClientWorkerInfo workerInfo : workerInfos) {
      nodeInfos[index ++] = new NodeInfo(workerInfo);
    }
    request.setAttribute("nodeInfos", nodeInfos);
  }
}
