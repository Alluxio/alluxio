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
    this.mMasterInfo = MI;
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
    long upMillis = System.currentTimeMillis() - this.mMasterInfo.getStarttimeMs();
    request.setAttribute("uptime", CommonUtils.convertMillis(upMillis));

    DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(this.mMasterInfo.getStarttimeMs());
    String startTime = formatter.format(cal.getTime());
    request.setAttribute("startTime", startTime);

    String version = Config.VERSION;
    request.setAttribute("version", version);

    String capacity = CommonUtils.getSizeFromBytes(this.mMasterInfo.getCapacityBytes());
    request.setAttribute("capacity", capacity);

    String usedCapacity = CommonUtils.getSizeFromBytes(this.mMasterInfo.getUsedCapacityBytes());
    request.setAttribute("usedCapacity", usedCapacity);

    String liveWorkerNodes = Integer.toString(this.mMasterInfo.getWorkerCount());
    request.setAttribute("liveWorkerNodes", liveWorkerNodes);

    List<ClientWorkerInfo> workerInfos = this.mMasterInfo.getWorkersInfo();
    int index = 0;
    NodeInfo[] nodeInfos = new NodeInfo[workerInfos.size()];
    for (ClientWorkerInfo workerInfo : workerInfos) {
      nodeInfos[index++] = new NodeInfo(workerInfo);
    }
    request.setAttribute("nodeInfos", nodeInfos);

    request.setAttribute("pinlist", this.mMasterInfo.getPinList());

    request.setAttribute("whitelist", this.mMasterInfo.getWhiteList());
    
  }

  // Class to make referencing worker nodes more intuitive. Mainly to avoid implicit association
  // by array indexes.

  public class NodeInfo {
    private String name;
    private String lastHeartbeat;
    private String state;
    private int percentFreeSpace;
    private int percentUsedSpace;

    private NodeInfo(ClientWorkerInfo workerInfo) {
      this.name = workerInfo.getAddress().getMHost();
      this.lastHeartbeat = Integer.toString(workerInfo.getLastContactSec());
      this.state = workerInfo.getState();
      Long usedSpace = 100*workerInfo.getUsedBytes()/workerInfo.getCapacityBytes();
      this.percentUsedSpace = usedSpace.intValue();
      this.percentFreeSpace = 100 - percentUsedSpace;
    }

    public String getName() {
      return this.name;
    }

    public String getLastHeartbeat() {
      return this.lastHeartbeat;
    }

    public String getState() {
      return this.state;
    }

    public int getPercentFreeSpace() {
      return this.percentFreeSpace;
    }

    public int getPercentUsedSpace() {
      return this.percentUsedSpace;
    }

  }
}