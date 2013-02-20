package tachyon;

import java.util.Calendar;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import javax.servlet.ServletException;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class WebInterfaceBrowseServlet extends HttpServlet {

  MasterInfo mMasterInfo;

	public WebInterfaceBrowseServlet(MasterInfo MI) {
		mMasterInfo = MI;
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response) 
    throws ServletException, IOException{
      String currentPath = request.getParameter("path");
      if (currentPath == null) {
      	currentPath = "/";
      }
      request.setAttribute("currentPath", currentPath);
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) {
	  return;
	}

}
