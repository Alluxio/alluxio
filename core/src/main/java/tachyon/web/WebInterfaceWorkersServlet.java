package tachyon.web;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.Ordering;

import tachyon.Constants;
import tachyon.master.MasterInfo;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.util.CommonUtils;

/**
 * Servlet that provides data for displaying detail info of all workers.
 */
public class WebInterfaceWorkersServlet extends HttpServlet {
  /**
   * Class to make referencing worker nodes more intuitive. Mainly to avoid implicit association by
   * array indexes.
   */
  public static class NodeInfo {
    private final String mName;
    private final String mLastContactSec;
    private final String mWorkerState;
    private final long mCapacityBytes;
    private final long mUsedBytes;
    private final int mFreePercent;
    private final int mUsedPercent;
    private final String mUptimeClockTime;

    private NodeInfo(ClientWorkerInfo workerInfo) {
      mName = workerInfo.getAddress().getMHost();
      mLastContactSec = Integer.toString(workerInfo.getLastContactSec());
      mWorkerState = workerInfo.getState();
      mCapacityBytes = workerInfo.getCapacityBytes();
      mUsedBytes = workerInfo.getUsedBytes();
      mUsedPercent = (int) (100L * mUsedBytes / mCapacityBytes);
      mFreePercent = 100 - mUsedPercent;
      mUptimeClockTime =
          CommonUtils.convertMsToShortClockTime(System.currentTimeMillis()
              - workerInfo.getStarttimeMs());
    }

    public String getCapacity() {
      return CommonUtils.getSizeFromBytes(mCapacityBytes);
    }

    public int getFreeSpacePercent() {
      return mFreePercent;
    }

    public String getLastHeartbeat() {
      return mLastContactSec;
    }

    public String getName() {
      return mName;
    }

    public String getState() {
      return mWorkerState;
    }

    public String getUptimeClockTime() {
      return mUptimeClockTime;
    }

    public String getUsedMemory() {
      return CommonUtils.getSizeFromBytes(mUsedBytes);
    }

    public int getUsedSpacePercent() {
      return mUsedPercent;
    }
  }

  private static final long serialVersionUID = -7454493761603179826L;

  private MasterInfo mMasterInfo;

  public WebInterfaceWorkersServlet(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }

  /**
   * Populates attributes before redirecting to a jsp.
   * 
   * @param request The HttpServletRequest object
   * @param response The HttpServletReponse object
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
   * @param workerInfos The list of ClientWorkerInfo objects
   * @return The list of NodeInfo objects
   */
  private NodeInfo[] generateOrderedNodeInfos(List<ClientWorkerInfo> workerInfos) {
    NodeInfo[] ret = new NodeInfo[workerInfos.size()];
    Collections.sort(workerInfos, new Ordering<ClientWorkerInfo>() {
      @Override
      public int compare(ClientWorkerInfo info0, ClientWorkerInfo info1) {
        return info0.getAddress().getMHost().compareTo(info1.getAddress().getMHost());
      }
    });
    int index = 0;
    for (ClientWorkerInfo workerInfo : workerInfos) {
      ret[index ++] = new NodeInfo(workerInfo);
    }

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

    List<ClientWorkerInfo> workerInfos = mMasterInfo.getWorkersInfo();
    NodeInfo[] normalNodeInfos = generateOrderedNodeInfos(workerInfos);
    request.setAttribute("normalNodeInfos", normalNodeInfos);

    List<ClientWorkerInfo> lostWorkerInfos = mMasterInfo.getLostWorkersInfo();
    NodeInfo[] failedNodeInfos = generateOrderedNodeInfos(lostWorkerInfos);
    request.setAttribute("failedNodeInfos", failedNodeInfos);
  }
}
