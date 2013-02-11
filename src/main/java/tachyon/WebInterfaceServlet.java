package tachyon;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class WebInterfaceServlet extends HttpServlet{
	protected void doGet(HttpServletRequest request, HttpServletResponse response) {
		request.getRequestDispatcher("/index.jsp");
	}
	protected void doPost(HttpServletRequest request, HttpServletResponse response) {
		return;
	}
}
