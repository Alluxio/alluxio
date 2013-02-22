package tachyon;

import java.lang.Comparable;
import java.util.List;
import java.util.Calendar;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import tachyon.thrift.ClientWorkerInfo;

public class WebInterfaceGeneralServlet extends HttpServlet {
  private MasterInfo mMasterInfo;

  public WebInterfaceGeneralServlet(MasterInfo MI) {
    mMasterInfo = MI;
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) 
      throws ServletException, IOException {
    this.populateValues(request);
    getServletContext().getRequestDispatcher("/general.jsp").forward(request, response);
  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    return;
  }

  // Populates key, value pairs for UI display
  private void populateValues(HttpServletRequest request) {
    long upMillis = System.currentTimeMillis() - mMasterInfo.getStarttimeMs();
    request.setAttribute("uptime", CommonUtils.convertMillis(upMillis));

    DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(mMasterInfo.getStarttimeMs());
    String startTime = formatter.format(cal.getTime());
    request.setAttribute("startTime", startTime);

    String version = Config.VERSION;
    request.setAttribute("version", version);

    String capacity = CommonUtils.getSizeFromBytes(mMasterInfo.getCapacityBytes());
    request.setAttribute("capacity", capacity);

    String usedCapacity = CommonUtils.getSizeFromBytes(mMasterInfo.getUsedCapacityBytes());
    request.setAttribute("usedCapacity", usedCapacity);

    String liveWorkerNodes = Integer.toString(mMasterInfo.getWorkerCount());
    request.setAttribute("liveWorkerNodes", liveWorkerNodes);

    List<ClientWorkerInfo> workerInfos = mMasterInfo.getWorkersInfo();
    int index = 0;
    NodeInfo[] nodeInfos = new NodeInfo[workerInfos.size()];
    for (ClientWorkerInfo workerInfo : workerInfos) {
      nodeInfos[index++] = new NodeInfo(workerInfo);
    }
    request.setAttribute("nodeInfos", nodeInfos);

    request.setAttribute("pinlist", mMasterInfo.getPinList());

    request.setAttribute("whitelist", mMasterInfo.getWhiteList());
    
  }

  // Class to make referencing worker nodes more intuitive. Mainly to avoid implicit association
  // by array indexes.

  public class NodeInfo {
    private String mName;
    private String mLastHeartbeat;
    private String mState;
    private int mPercentFreeSpace;
    private int mPercentUsedSpace;

    private NodeInfo(ClientWorkerInfo workerInfo) {
      mName = workerInfo.getAddress().getMHost();
      mLastHeartbeat = Integer.toString(workerInfo.getLastContactSec());
      mState = workerInfo.getState();
      Long usedSpace = 100*workerInfo.getUsedBytes()/workerInfo.getCapacityBytes();
      mPercentUsedSpace = usedSpace.intValue();
      mPercentFreeSpace = 100 - mPercentUsedSpace;
    }

    public String getName() {
      return mName;
    }

    public String getLastHeartbeat() {
      return mLastHeartbeat;
    }

    public String getState() {
      return mState;
    }

    public int getPercentFreeSpace() {
      return mPercentFreeSpace;
    }

    public int getPercentUsedSpace() {
      return mPercentUsedSpace;
    }

  }
}