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

import com.google.common.base.Objects;

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
  public static final class NodeInfo implements Comparable<NodeInfo> {
    private final String mHost;
    private final int mWebPort;
    private final String mLastContactSec;
    private final String mWorkerState;
    private final long mCapacityBytes;
    private final long mUsedBytes;
    private final int mFreePercent;
    private final int mUsedPercent;
    private final String mUptimeClockTime;

    private NodeInfo(WorkerInfo workerInfo) {
      mHost = workerInfo.getAddress().getHost();
      mWebPort = workerInfo.getAddress().getWebPort();
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
          WebUtils.convertMsToShortClockTime(
              System.currentTimeMillis() - workerInfo.getStartTimeMs());
    }

    /**
     * @return the worker capacity in bytes
     */
    public String getCapacity() {
      return FormatUtils.getSizeFromBytes(mCapacityBytes);
    }

    /**
     * @return the worker free space as a percentage
     */
    public int getFreeSpacePercent() {
      return mFreePercent;
    }

    /**
     * @return the time of the last worker heartbeat
     */
    public String getLastHeartbeat() {
      return mLastContactSec;
    }

    /**
     * @return the worker host
     */
    public String getHost() {
      return mHost;
    }

    /**
     * @return the worker port
     */
    public int getWebPort() {
      return mWebPort;
    }

    /**
     * @return the worker state
     */
    public String getState() {
      return mWorkerState;
    }

    /**
     * @return the worker uptime
     */
    public String getUptimeClockTime() {
      return mUptimeClockTime;
    }

    /**
     * @return the worker used capacity in bytes
     */
    public String getUsedMemory() {
      return FormatUtils.getSizeFromBytes(mUsedBytes);
    }

    /**
     * @return the worker used space as a percentage
     */
    public int getUsedSpacePercent() {
      return mUsedPercent;
    }

    /**
     * Compare {@link NodeInfo} by lexicographical order of their associated host
     *
     * @param o the comparison term
     * @return a positive value if {@code this.getHost} is lexicographically "bigger" than
     *         {@code o.getHost}, 0 if the hosts are equal, a negative value otherwise.
     */
    @Override
    public int compareTo(NodeInfo o) {
      if (o == null) {
        return 1;
      } else {
        return this.getHost().compareTo(o.getHost());
      }
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof NodeInfo)) {
        return false;
      }
      return this.getHost().equals(((NodeInfo) o).getHost());
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(this.getHost());
    }
  }

  private static final long serialVersionUID = -7454493761603179826L;

  private final transient BlockMaster mBlockMaster;
  private final transient TachyonConf mTachyonConf;

  /**
   * Creates a new instance of {@link WebInterfaceWorkersServlet}
   *
   * @param blockMaster block master
   */
  public WebInterfaceWorkersServlet(BlockMaster blockMaster) {
    mBlockMaster = blockMaster;
    mTachyonConf = new TachyonConf();
  }

  /**
   * Populates attributes before redirecting to a jsp.
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
    getServletContext().getRequestDispatcher("/workers.jsp").forward(request, response);
  }

  /**
   * Order the nodes by hostName and generate {@link NodeInfo} list for UI display.
   *
   * @param workerInfos the list of {@link WorkerInfo} objects
   * @return the list of {@link NodeInfo} objects
   */
  private NodeInfo[] generateOrderedNodeInfos(Collection<WorkerInfo> workerInfos) {
    NodeInfo[] ret = new NodeInfo[workerInfos.size()];
    int index = 0;
    for (WorkerInfo workerInfo : workerInfos) {
      ret[index ++] = new NodeInfo(workerInfo);
    }
    Arrays.sort(ret);

    return ret;
  }

  /**
   * Populates key, value pairs for UI display.
   *
   * @param request the {@link HttpServletRequest} object
   * @throws IOException if an I/O error occurs
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
