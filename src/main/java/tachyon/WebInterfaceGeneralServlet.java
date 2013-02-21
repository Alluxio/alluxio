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
	  int upMillis = (int) (System.currentTimeMillis() - mMasterInfo.getStarttimeMs());
    String uptime = (upMillis/84600000) + " d "
    				        + (upMillis/3600000 % 24) + " h "
    				        + (upMillis/60000 % 60) + " m "
    				        + (upMillis/1000 % 60) + " s ";
    request.setAttribute("uptime", uptime);
      
    DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(mMasterInfo.getStarttimeMs());
    String startTime = formatter.format(cal.getTime());
    request.setAttribute("startTime", startTime);

    String version = Config.VERSION;
    request.setAttribute("version", version);

    String capacity = Double.toString(mMasterInfo.getCapacityBytes()/Math.pow(2,30));
    request.setAttribute("capacity", capacity);

    String usedCapacity = Double.toString(mMasterInfo.getUsedCapacityBytes()/Math.pow(2,30));
    request.setAttribute("usedCapacity", usedCapacity);

    String liveWorkerNodes = Integer.toString(mMasterInfo.getWorkerCount());
    request.setAttribute("liveWorkerNodes", liveWorkerNodes);


	}
}
