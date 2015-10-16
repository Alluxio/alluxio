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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.Ordering;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.master.block.BlockMaster;
import tachyon.thrift.WorkerInfo;
import tachyon.util.FormatUtils;

/**
 * Servlet that provides data for displaying detail info of all workers.
 */
public final class WebInterfaceWorkersServlet extends HttpServlet {
  /**
   * Class to make referencing worker nodes more intuitive. Mainly to avoid implicit association by
   * array indexes.
   */
  public static class NodeInfo {
    private final String mHost;
    private final String mLastContactSec;
    private final String mWorkerState;
    private final long mCapacityBytes;
    private final long mUsedBytes;
    private final int mFreePercent;
    private final int mUsedPercent;
    private final String mUptimeClockTime;

    private NodeInfo(WorkerInfo workerInfo) {
      mHost = workerInfo.getAddress().getHost();
      mLastContactSec = Integer.toString(workerInfo.getLastContactSec());
      mWorkerState = workerInfo.getState();
      mCapacityBytes = workerInfo.getCapacityBytes();
      mUsedBytes = workerInfo.getUsedBytes();
      if (mCapacityBytes != 0) {
        mUsedPercent = (int) (100L * mUsedBytes / mCapacityBytes);
      } else {
        mUsedPercent = 0;
      }
      mFreePercent = 100 - mUsedPercent;
      mUptimeClockTime =
          Utils.convertMsToShortClockTime(System.currentTimeMillis() - workerInfo.getStartTimeMs());
    }

    public String getCapacity() {
      return FormatUtils.getSizeFromBytes(mCapacityBytes);
    }

    public int getFreeSpacePercent() {
      return mFreePercent;
    }

    public String getLastHeartbeat() {
      return mLastContactSec;
    }

    public String getHost() {
      return mHost;
    }

    public String getState() {
      return mWorkerState;
    }

    public String getUptimeClockTime() {
      return mUptimeClockTime;
    }

    public String getUsedMemory() {
      return FormatUtils.getSizeFromBytes(mUsedBytes);
    }

    public int getUsedSpacePercent() {
      return mUsedPercent;
    }
  }

  private static final long serialVersionUID = -7454493761603179826L;

  private final transient BlockMaster mBlockMaster;
  private final transient TachyonConf mTachyonConf;

  public WebInterfaceWorkersServlet(BlockMaster blockMaster) {
    mBlockMaster = blockMaster;
    mTachyonConf = new TachyonConf();
  }

  /**
   * Populates attributes before redirecting to a jsp.
   *
   * @param request The HttpServletRequest object
   * @param response The HttpServletReponse object
   * @throws ServletException
   * @throws IOException
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    populateValues(request);
    getServletContext().getRequestDispatcher("/workers.jsp").forward(request, response);
  }

  /**
   * Order the nodes by hostName and generate NodeInfo list for UI display
   *
   * @param workerInfos The list of WorkerInfo objects
   * @return The list of NodeInfo objects
   */
  private NodeInfo[] generateOrderedNodeInfos(Collection<WorkerInfo> workerInfos) {
    NodeInfo[] ret = new NodeInfo[workerInfos.size()];
    int index = 0;
    for (WorkerInfo workerInfo : workerInfos) {
      ret[index ++] = new NodeInfo(workerInfo);
    }

    Arrays.sort(ret, new Ordering<NodeInfo>() {
      @Override
      public int compare(NodeInfo info0, NodeInfo info1) {
        return info0.getHost().compareTo(info1.getHost());
      }
    });

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

    List<WorkerInfo> workerInfos = mBlockMaster.getWorkerInfoList();
    NodeInfo[] normalNodeInfos = generateOrderedNodeInfos(workerInfos);
    request.setAttribute("normalNodeInfos", normalNodeInfos);

    Set<WorkerInfo> lostWorkerInfos = mBlockMaster.getLostWorkersInfo();
    NodeInfo[] failedNodeInfos = generateOrderedNodeInfos(lostWorkerInfos);
    request.setAttribute("failedNodeInfos", failedNodeInfos);

    request.setAttribute("workerWebPort", mTachyonConf.getInt(Constants.WORKER_WEB_PORT));
  }
}
