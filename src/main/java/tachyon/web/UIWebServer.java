package tachyon.web;

import java.io.File;
import java.net.InetSocketAddress;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;
import org.apache.log4j.Logger;

import tachyon.CommonUtils;
import tachyon.Constants;
import tachyon.MasterInfo;

/**
 * Class that bootstraps and starts the web server for the web interface.
 */
public class UIWebServer {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private Server mServer;
  private String mServerName;
  private InetSocketAddress mAddress;

  /**
   * Constructor that pairs urls with servlets and sets the webapp folder.
   * @param serverName Name of the server
   * @param InetSocketAddress Address of the server
   * @param masterInfo MasterInfo for the tachyon filesystem this UIWebServer supports
   * @return A new UIWebServer
   */
  public UIWebServer(String serverName, InetSocketAddress address, MasterInfo masterInfo) {
    mAddress = address;
    mServerName = serverName;
    mServer = new Server(mAddress);

    WebAppContext webappcontext = new WebAppContext();

    webappcontext.setContextPath("/");
    File warPath = new File(new File("").getAbsolutePath(), "src/main/java/tachyon/web/resources");
    webappcontext.setWar(warPath.getAbsolutePath());
    HandlerList handlers = new HandlerList();
    webappcontext.addServlet(
        new ServletHolder(new WebInterfaceGeneralServlet(masterInfo)), "/home");
    webappcontext.addServlet(
        new ServletHolder(new WebInterfaceBrowseServlet(masterInfo)), "/browse");
    webappcontext.addServlet(
        new ServletHolder(new WebInterfaceMemoryServlet(masterInfo)), "/memory");

    handlers.setHandlers(new Handler[] { webappcontext, new DefaultHandler() });
    mServer.setHandler(handlers);
  }

  public void setHandler(AbstractHandler handler) {
    mServer.setHandler(handler);
  }

  public void startWebServer() {
    try {
      mServer.start();
      LOG.info(mServerName + " started @ " + mAddress);
    } catch (Exception e) {
      CommonUtils.runtimeException(e);
    }
  }

  public void shutdownWebServer() throws Exception {
    mServer.stop();
  }
}