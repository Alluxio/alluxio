package tachyon.web;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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

    public String getUsedMemory() {
      return CommonUtils.getSizeFromBytes(mUsedBytes);
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
   * @param request
   *          The HttpServletRequest object
   * @param response
   *          The HttpServletReponse object
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    populateValues(request);
    getServletContext().getRequestDispatcher("/workers.jsp").forward(request, response);
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

    List<ClientWorkerInfo> workerInfos = mMasterInfo.getWorkersInfo();
    for (int i = 0; i < workerInfos.size(); i++) {
      for (int j = i + 1; j < workerInfos.size(); j++) {
        if (workerInfos.get(i).getAddress().getMHost()
            .compareTo(workerInfos.get(j).getAddress().getMHost()) > 0) {
          ClientWorkerInfo temp = workerInfos.get(i);
          workerInfos.set(i, workerInfos.get(j));
          workerInfos.set(j, temp);
        }
      }
    }
    int index = 0;
    NodeInfo[] normalNodeInfos = new NodeInfo[workerInfos.size()];
    for (ClientWorkerInfo workerInfo : workerInfos) {
      normalNodeInfos[index ++] = new NodeInfo(workerInfo);
    }
    request.setAttribute("normalNodeInfos", normalNodeInfos);

    List<ClientWorkerInfo> lostWorkerInfos = mMasterInfo.getLostWorkersInfo();
    for (int i = 0; i < lostWorkerInfos.size(); i++) {
      for (int j = i + 1; j < lostWorkerInfos.size(); j++) {
        if (lostWorkerInfos.get(i).getAddress().getMHost()
            .compareTo(lostWorkerInfos.get(j).getAddress().getMHost()) > 0) {
          ClientWorkerInfo temp = lostWorkerInfos.get(i);
          lostWorkerInfos.set(i, lostWorkerInfos.get(j));
          lostWorkerInfos.set(j, temp);
        }
      }
    }
    index = 0;
    NodeInfo[] failedNodeInfos = new NodeInfo[lostWorkerInfos.size()];
    for (ClientWorkerInfo workerInfo : lostWorkerInfos) {
      failedNodeInfos[index ++] = new NodeInfo(workerInfo);
    }
    request.setAttribute("failedNodeInfos", failedNodeInfos);
  }
}
