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
import alluxio.master.block.BlockMaster;
import alluxio.util.FormatUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Objects;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet that provides data for displaying detail info of all workers.
 */
@ThreadSafe
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
    private final long mWorkerId;

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
      mWorkerId = workerInfo.getId();
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
     * @return the worker id
     */
    public long getWorkerId() {
      return mWorkerId;
    }

    /**
     * Compare {@link NodeInfo} by lexicographical order of their associated host.
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

  private final BlockMaster mBlockMaster;

  /**
   * Creates a new instance of {@link WebInterfaceWorkersServlet}.
   *
   * @param blockMaster block master
   */
  public WebInterfaceWorkersServlet(BlockMaster blockMaster) {
    mBlockMaster = blockMaster;
  }

  /**
   * Populates attributes before redirecting to a jsp.
   *
   * @param request the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   * @throws ServletException if the target resource throws this exception
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
      ret[index++] = new NodeInfo(workerInfo);
    }
    Arrays.sort(ret);

    return ret;
  }

  /**
   * Populates key, value pairs for UI display.
   *
   * @param request the {@link HttpServletRequest} object
   */
  private void populateValues(HttpServletRequest request) throws IOException {
    request.setAttribute("debug", Configuration.getBoolean(PropertyKey.DEBUG));

    List<WorkerInfo> workerInfos = mBlockMaster.getWorkerInfoList();
    NodeInfo[] normalNodeInfos = generateOrderedNodeInfos(workerInfos);
    request.setAttribute("normalNodeInfos", normalNodeInfos);

    List<WorkerInfo> lostWorkerInfos = mBlockMaster.getLostWorkersInfoList();
    NodeInfo[] failedNodeInfos = generateOrderedNodeInfos(lostWorkerInfos);
    request.setAttribute("failedNodeInfos", failedNodeInfos);
  }
}
