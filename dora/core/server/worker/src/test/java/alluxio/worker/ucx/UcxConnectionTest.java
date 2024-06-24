package alluxio.worker.ucx;

import alluxio.concurrent.jsr.CompletableFuture;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.apache.log4j.PropertyConfigurator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openucx.jucx.ucp.UcpConnectionRequest;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpListener;
import org.openucx.jucx.ucp.UcpListenerConnectionHandler;
import org.openucx.jucx.ucp.UcpListenerParams;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Properties;

public class UcxConnectionTest {
  public static UcpContext sGlobalContext;
  private static Logger LOG = LoggerFactory.getLogger(UcxConnectionTest.class);


  @BeforeClass
  public static void initContext() {
    PropertyConfigurator.configure("/root/github/alluxio/conf/log4j.properties");
    Properties props = new Properties();
    props.setProperty(PropertyKey.LOGGER_TYPE.toString(), "Console");
    System.out.println("start initContext...");

    sGlobalContext = new UcpContext(new UcpParams()
        .requestStreamFeature()
        .requestTagFeature()
        .requestWakeupFeature());
  }


  @Test
  public void testEstablishConnection() throws Exception {
    InetAddress localAddr = InetAddress.getLocalHost();
    int serverPort = 1234;
    UcpWorker serverWorker = sGlobalContext.newWorker(new UcpWorkerParams().requestThreadSafety());
    CompletableFuture<UcpConnectionRequest> incomingConn = new CompletableFuture<>();
    UcpListenerParams listenerParams = new UcpListenerParams()
        .setConnectionHandler(new UcpListenerConnectionHandler() {
          @Override
          public void onConnectionRequest(UcpConnectionRequest connectionRequest) {
            System.out.println("Got incoming req...");
            incomingConn.complete(connectionRequest);
          }
        });
    InetSocketAddress remoteAddr = new InetSocketAddress(localAddr, serverPort);
    UcpListener ucpListener = serverWorker.newListener(
        listenerParams.setSockAddr(remoteAddr));
    System.out.println("Bound UcpListener on address:" + ucpListener.getAddress());
    Thread serverThread = new Thread(() -> {
      try {
        while (serverWorker.progress() == 0) {
          System.out.println("Nothing to progress, waiting on events..");
          serverWorker.waitForEvents();
        }
        UcpConnectionRequest incomeConnReq = incomingConn.get();
        if (incomeConnReq != null) {
          UcpEndpoint bootstrapEp = serverWorker.newEndpoint(new UcpEndpointParams()
              .setPeerErrorHandlingMode()
              .setConnectionRequest(incomeConnReq));
          UcxConnection ucxConnection = UcxConnection.acceptIncomingConnection(
              bootstrapEp, serverWorker, incomeConnReq.getClientAddress());
          System.out.println("Conn established from server:" + ucxConnection.toString());
        }
      } catch (Exception e) {
        System.out.println("Exception in server thread.");
        e.printStackTrace();
      }
    });
    serverThread.start();

    UcpWorker clientWorker = sGlobalContext.newWorker(new UcpWorkerParams().requestThreadSafety());
    // client init conn
    System.out.println("Starting init new conn...");
    System.out.println("remoteaddr:" + remoteAddr.toString());
    UcxConnection connToServer = UcxConnection.initNewConnection(remoteAddr, clientWorker);
    System.out.println("Conn established to server:" + connToServer.toString());
    serverThread.join();
  }
}
