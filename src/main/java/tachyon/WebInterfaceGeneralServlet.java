package tachyon;

import java.util.Calendar;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class WebInterfaceGeneralServlet extends HttpServlet {
  MasterInfo mMasterInfo;

  public WebInterfaceGeneralServlet(MasterInfo MI) {
    mMasterInfo = MI;
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) 
      throws ServletException, IOException{
    populateValues(request);
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
  }
}