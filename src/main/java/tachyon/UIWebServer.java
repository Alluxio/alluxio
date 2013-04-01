package tachyon;

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

public class UIWebServer {
  private final Logger LOG = Logger.getLogger(Config.LOGGER_TYPE);

  private Server mServer;
  private String mServerName;
  private InetSocketAddress mAddress;

  public UIWebServer(String serverName, InetSocketAddress address, MasterInfo masterInfo) {
    mAddress = address;
    mServerName = serverName;
    mServer = new Server(mAddress);

    WebAppContext webappcontext = new WebAppContext();

    webappcontext.setContextPath("/");
    File warPath = new File(new File("").getAbsolutePath(), "src/main/java/tachyon/webapps");
    webappcontext.setWar(warPath.getAbsolutePath());
    HandlerList handlers = new HandlerList();
    webappcontext.addServlet(
        new ServletHolder(new WebInterfaceGeneralServlet(masterInfo)), "/home");
    webappcontext.addServlet(
        new ServletHolder(new WebInterfaceBrowseServlet(masterInfo)), "/browse");
    webappcontext.addServlet(
        new ServletHolder(new WebInterfaceMemoryServlet(masterInfo)), "/memory");
    webappcontext.addServlet(
        new ServletHolder(new WebInterfaceDependencyServlet(masterInfo)), "/dependency");

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