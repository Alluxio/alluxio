/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.logserver;

import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.apache.log4j.Hierarchy;
import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.log4j.spi.RootLogger;

import com.google.common.base.Function;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.NumberFormatException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Properties;

/**
 * A centralized log server for Alluxio
 *
 * Alluxio masters and workers generate logs and store the logs in local storage. {@link AlluxioLogServerProcess}
 * allows masters and workers to "push" their logs to a centralized log server where another copy of the logs will
 * be stored.
 */
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
    CommonUtils.waitFor(this + " to start", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return mServerSocket != null;
      }
    }, WaitForOptions.defaults().setTimeoutMs(10000));
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
