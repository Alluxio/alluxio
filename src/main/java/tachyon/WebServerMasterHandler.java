package tachyon;

import org.eclipse.jetty.servlet.ServletHandler;


public class WebServerMasterHandler extends ServletHandler {
  private MasterServiceHandler mMasterServiceHandler;
  
  public WebServerMasterHandler(MasterServiceHandler msh) {
    mMasterServiceHandler = msh;
  }
}
