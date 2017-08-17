package alluxio.logserver;

import org.apache.log4j.Hierarchy;
import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.log4j.spi.RootLogger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.NumberFormatException;
import java.net.*;
import java.util.HashMap;
import java.util.Properties;

public class AlluxioLogServerProcess implements LogServerProcess {
  private ServerSocket mServerSocket;
  private int mPort;
  private String mBaseLogDir;
  private HashMap<InetAddress, Hierarchy> mInetAddressHashMap;
  private boolean mStopped;

  public AlluxioLogServerProcess(String portStr, String baseLogDir) throws NumberFormatException, IOException {
    mPort = Integer.parseInt(portStr);
    mBaseLogDir = baseLogDir;
    mServerSocket = new ServerSocket(mPort);
    mInetAddressHashMap = new HashMap<>();
    mStopped = false;
  }
  /**
   * Starts the Alluxio process. This call blocks until the process is stopped via
   * {@link #stop()}. The {@link #waitForReady()} method can be used to make sure that the
   * process is ready to serve requests.
   */
  @Override
  public void start() throws Exception {
    while (!mStopped) {
      Socket client = mServerSocket.accept();
      InetAddress inetAddress = client.getInetAddress();
      LoggerRepository loggerRepository = mInetAddressHashMap.get(inetAddress);
      if (loggerRepository == null) {
        loggerRepository = configureHierarchy(inetAddress);
      }
      new Thread(new AlluxioLog4jSocketNode(client, loggerRepository)).start();
    }
  }

  /**
   * Stops the Alluxio process, blocking until the action is completed.
   */
  @Override
  public void stop() throws Exception {
    mStopped = true;
  }

  /**
   * Waits until the process is ready to serve requests.
   */
  @Override
  public void waitForReady() {
  }

  private LoggerRepository configureHierarchy(InetAddress inetAddress) throws IOException, URISyntaxException {
    Hierarchy clientHierarchy;
    String inetAddressStr = inetAddress.toString();
    int i = inetAddressStr.indexOf("/");
    String key;
    if (i == 0) {
      key = inetAddressStr.substring(1, inetAddressStr.length());
    } else {
      key = inetAddressStr.substring(0, i);
    }
    Properties properties = new Properties();
    File configFile = new File(new URI(System.getProperty("log4j.configuration")));
    properties.load(new FileInputStream(configFile));
    clientHierarchy = new Hierarchy(new RootLogger(Level.INFO));
    String logFilePath = mBaseLogDir;
    String loggerType = System.getProperty("alluxio.logger.type");
    if (loggerType.contains("MASTER")) {
      logFilePath += ("/master_logs/" + key + ".master.log");
    } else if (loggerType.contains("WORKER")) {
      logFilePath += ("/worker_logs/" + key + ".worker.log");
    } else {
      // Should not reach here
    }
    properties.setProperty("log4j.appender." + loggerType + ".File", logFilePath);
    new PropertyConfigurator().doConfigure(properties, clientHierarchy);
    return clientHierarchy;
  }
}
