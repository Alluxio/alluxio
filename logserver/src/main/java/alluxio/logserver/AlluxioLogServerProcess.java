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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A centralized log server for Alluxio
 *
 * Alluxio masters and workers generate logs and store the logs in local storage.
 * {@link AlluxioLogServerProcess} allows masters and workers to "push" their logs to a
 * centralized log server where another copy of the logs will be stored.
 */
public class AlluxioLogServerProcess implements LogServerProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioLogServer.class);
  private final int mRemoteMastersLoggingPort;
  private final int mRemoteWorkersLoggingPort;
  private String mBaseLogsDir;
  private Thread mMastersLogThread;
  private Thread mWorkersLogThread;
  private LogReceiver mMastersLogReceiver;
  private LogReceiver mWorkersLogReceiver;
  private volatile boolean mStopped;

  /**
   * Construct an {@link AlluxioLogServerProcess} instance.
   *
   * @param baseLogsDir base directory to store the logs pushed from remote Alluxio servers
   */
  public AlluxioLogServerProcess(String baseLogsDir) {
    mRemoteMastersLoggingPort = 47120;
    mRemoteWorkersLoggingPort = 47121;
    mBaseLogsDir = baseLogsDir;
    mStopped = true;
  }

  @Override
  public void start() throws Exception {
    startLogServerThreads();
    LOG.info("Log server started");
    try {
      mMastersLogThread.join();
      mWorkersLogThread.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void stop() throws Exception {
    mStopped = true;
    LOG.info("Log server stopped.");
  }

  @Override
  public void waitForReady() {
    CommonUtils.waitFor(this + " to start", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return mStopped == false;
      }
    }, WaitForOptions.defaults().setTimeoutMs(10000));
  }

  /**
   * Create and start logging server threads.
   */
  private void startLogServerThreads() {
    // TODO(yanqin) make it configurable.
    mMastersLogReceiver = new LogReceiver(mRemoteMastersLoggingPort, "MASTER_LOGGER");
    mWorkersLogReceiver = new LogReceiver(mRemoteWorkersLoggingPort, "WORKER_LOGGER");
    mMastersLogThread = new Thread(mMastersLogReceiver);
    mWorkersLogThread = new Thread(mWorkersLogReceiver);
    mStopped = false;
    mMastersLogThread.start();
    mWorkersLogThread.start();
  }

  /**
   * Configure a {@link Hierarchy} instance used to retrive logger by name and maintain the logger
   * hierarchy. An instance of this class will be passed to a {@link AlluxioLog4jSocketNode} so
   * that the {@link AlluxioLog4jSocketNode} instance can retrieve the logger to log incoming
   * {@link org.apache.log4j.spi.LoggingEvent}s.
   *
   * @param inetAddress inet address of the client
   * @param loggerType type of the appender to use for this client, e.g. MASTER_LOGGER, etc
   * @return A {@link Hierarchy} instance to pass to {@link AlluxioLog4jSocketNode}
   * @throws IOException if fails to create an {@link FileInputStream} to read log4j.properties
   * @throws URISyntaxException if fails to derive a valid URI from the value of property named
   *         "log4j.configuration"
   */
  private LoggerRepository configureHierarchy(InetAddress inetAddress, String loggerType)
      throws IOException, URISyntaxException {
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
    try (FileInputStream inputStream = new FileInputStream(configFile)) {
      properties.load(inputStream);
    }
    clientHierarchy = new Hierarchy(new RootLogger(Level.INFO));
    String logFilePath = mBaseLogsDir;
    if (loggerType.contains("MASTER")) {
      logFilePath += ("/master_logs/" + key + ".master.log");
    } else if (loggerType.contains("WORKER")) {
      logFilePath += ("/worker_logs/" + key + ".worker.log");
    } else {
      // Should not reach here
      throw new IllegalStateException("Unknown logger type");
    }
    properties.setProperty("log4j.rootLogger", "INFO," + loggerType);
    properties.setProperty("log4j.appender." + loggerType + ".File", logFilePath);
    new PropertyConfigurator().doConfigure(properties, clientHierarchy);
    return clientHierarchy;
  }

  /**
   * Runnable object that serves logging clients, i.e. Alluxio servers in this case.
   */
  private class LogReceiver implements Runnable {
    private ServerSocket mServerSocket;
    private final int mPort;
    private final String mLoggerType;
    private Map<InetAddress, LoggerRepository> mInetAddressHashMap;

    /**
     * Construct {@link LogReceiver} instance.
     *
     * @param port port number to bind
     * @param loggerType string representation of the type of the appender,
     *                   e.g. MASTER_LOGGER, WORKER_LOGGER, etc
     */
    public LogReceiver(int port, String loggerType) {
      mPort = port;
      mLoggerType = loggerType;
      mInetAddressHashMap = new HashMap<>();
    }

    @Override
    public void run() {
      try {
        mServerSocket = new ServerSocket(mPort);
      } catch (IOException e) {
        LOG.error("Failed to bind to port {}.", mPort);
        return;
      }
      while (!mStopped) {
        try {
          Socket client = mServerSocket.accept();
          InetAddress inetAddress = client.getInetAddress();
          // Currently loggerRepository is always null because we are not using the map.
          LoggerRepository loggerRepository = mInetAddressHashMap.get(inetAddress);
          if (loggerRepository == null) {
            try {
              loggerRepository = configureHierarchy(inetAddress, mLoggerType);
              mInetAddressHashMap.put(inetAddress, loggerRepository);
            } catch (URISyntaxException e) {
              LOG.warn("URI syntax error.");
              continue;
            }
          }
          try {
            new Thread(new AlluxioLog4jSocketNode(client, loggerRepository)).start();
            LOG.info("Client: {} connected.", inetAddress.toString());
          } catch (IOException e) {
            // Could not create a thread to serve a client, ignore this client.
            LOG.warn("Failed to connect with client: {}.", inetAddress.toString());
            continue;
          }
        } catch (IOException e) {
          break;
        }
      }
    }
  }
}
