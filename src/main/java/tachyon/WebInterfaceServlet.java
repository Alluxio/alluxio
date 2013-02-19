package tachyon;

import java.util.Calendar;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class WebInterfaceServlet extends HttpServlet {

  MasterInfo mMSH;

	public WebInterfaceServlet(MasterInfo MSH) {
		this.mMSH = MSH;
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
	  int upMillis = (int) (System.currentTimeMillis() - mMSH.getStarttimeMs());
      String uptime = (upMillis/84600000) + " d "
      				  + (upMillis/3600000 % 24) + " h "
      				  + (upMillis/60000 % 60) + " m "
      				  + (upMillis/1000 % 60) + " s ";
      request.setAttribute("uptime", uptime);
      
      DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(mMSH.getStarttimeMs());
      String startTime = formatter.format(cal.getTime());
      request.setAttribute("startTime", startTime);

	}
}
