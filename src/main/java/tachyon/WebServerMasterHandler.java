package tachyon;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;


public class WebServerMasterHandler extends AbstractHandler {
  private MasterServiceHandler mMasterServiceHandler;
  
  @Override
  public void handle(String target, Request baseRequest,
      HttpServletRequest request, HttpServletResponse response) 
          throws IOException, ServletException {
	response.setContentType("text/html;charset=utf-8");
    response.setStatus(HttpServletResponse.SC_OK);
    baseRequest.setHandled(true);
    response.getWriter().println(mMasterServiceHandler.toHtml());
  }
  
  public WebServerMasterHandler(MasterServiceHandler msh) {
    mMasterServiceHandler = msh;
  }
}