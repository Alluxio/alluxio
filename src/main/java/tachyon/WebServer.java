package tachyon;

import java.net.InetSocketAddress;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WebServer {
  private final Logger LOG = LoggerFactory.getLogger(WebServer.class);

  private Server mServer;
  private String mServerName;
  private InetSocketAddress mAddress;

  public WebServer(String serverName, InetSocketAddress address) {
    mAddress = address;
    mServerName = serverName;
    mServer = new Server(mAddress);
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