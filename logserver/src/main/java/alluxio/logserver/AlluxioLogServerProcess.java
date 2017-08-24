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

import alluxio.Configuration;
import alluxio.PropertyKey;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

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
  private final int mMinNumberOfThreads;
  private final int mMaxNumberOfThreads;
  private ExecutorService mThreadPool;
  private volatile boolean mStopped;

  private long mStopTimeoutInMillis = 60000;
  private long mRequestTimeoutInMillis = 20000;
  private int mBackoffSlotInMillis = 100;
  private Random mRandom = new Random(System.currentTimeMillis());

  /**
   * Construct an {@link AlluxioLogServerProcess} instance.
   *
   * @param baseLogsDir base directory to store the logs pushed from remote Alluxio servers
   */
  public AlluxioLogServerProcess(String baseLogsDir) {
    // TODO(yanqin) make it configurable.
    mRemoteMastersLoggingPort = Configuration.getInt(PropertyKey.LOG_SERVER_MASTERS_LOGGING_PORT);
    mRemoteWorkersLoggingPort = Configuration.getInt(PropertyKey.LOG_SERVER_WORKERS_LOGGING_PORT);
    mMinNumberOfThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MIN) + 2;
    mMaxNumberOfThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MAX) + 2;
    mBaseLogsDir = baseLogsDir;
    mStopped = true;
  }

  @Override
  public void start() throws Exception {
    SynchronousQueue<Runnable> synchronousQueue =
        new SynchronousQueue<>();
    mThreadPool =
        new ThreadPoolExecutor(mMinNumberOfThreads, mMaxNumberOfThreads,
            60, TimeUnit.SECONDS, synchronousQueue);
    startLogServerThreads();
  }

  @Override
  public void stop() throws Exception {
    mStopped = true;
    stopLogServerThreads();
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
    mMastersLogReceiver = new LogReceiver(mRemoteMastersLoggingPort, "MASTER_LOGGER");
    mWorkersLogReceiver = new LogReceiver(mRemoteWorkersLoggingPort, "WORKER_LOGGER");
    mMastersLogThread = new Thread(mMastersLogReceiver);
    mWorkersLogThread = new Thread(mWorkersLogReceiver);
    mStopped = false;
    mMastersLogThread.start();
    mWorkersLogThread.start();
    while (!Thread.interrupted()) {
      CommonUtils.sleepMs(LOG, 1000);
    }
  }

  private void stopLogServerThreads() {
    mMastersLogReceiver.close();
    mWorkersLogReceiver.close();
    try {
      mMastersLogThread.join();
      mWorkersLogThread.join();
      mMastersLogThread = null;
      mWorkersLogThread = null;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
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

    public void close() {
      if (mServerSocket != null) {
        try {
          mServerSocket.close();
        } catch (IOException e) {
          return;
        }
      }
    }

    @Override
    public void run() {
      try {
        mServerSocket = new ServerSocket(mPort);
      } catch (IOException e) {
        LOG.error("Failed to bind to port {}.", mPort);
        return;
      }
      int failureCount = 0;
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
          AlluxioLog4jSocketNode clientSocketNode =
              new AlluxioLog4jSocketNode(client, loggerRepository);
          int retryCount = 0;
          long remainTimeInMillis = mRequestTimeoutInMillis;
          while (true) {
            try {
              mThreadPool.execute(clientSocketNode);
              break;
            } catch (Throwable t) {
              if (t instanceof RejectedExecutionException) {
                retryCount++;
                try {
                  if (remainTimeInMillis > 0) {
                    long sleepTimeInMillis = ((long) (mRandom.nextDouble()
                        * (1L << Math.min(retryCount, 20)))) * mBackoffSlotInMillis;
                    sleepTimeInMillis = Math.min(sleepTimeInMillis, remainTimeInMillis);
                    TimeUnit.MILLISECONDS.sleep(sleepTimeInMillis);
                    remainTimeInMillis -= sleepTimeInMillis;
                  } else {
                    client.close();
                    LOG.warn("Connection has been rejected by ExecutorService "
                        + retryCount + " times till timedout, reason: " + t);
                    break;
                  }
                } catch (InterruptedException e) {
                  LOG.warn("Interrupted while waiting to place client on executor queue.");
                  Thread.currentThread().interrupt();
                  break;
                }
              } else if (t instanceof  Error) {
                LOG.error("ExecutorService threw error: " + t, t);
                throw (Error) t;
              } else {
                LOG.warn("ExecutorService threw error: " + t, t);
                break;
              }
              // Could not create a thread to serve a client, ignore this client.
              LOG.warn("Failed to connect with client: {}.", inetAddress.toString());
              continue;
            }
          }
        } catch (IOException e) {
          if (!mStopped) {
            ++failureCount;
            LOG.warn("Socket transport error occurred during accepting message.", e);
          }
          break;
        }
      }

      mThreadPool.shutdown();
      long timeoutMS = mStopTimeoutInMillis;
      long now = System.currentTimeMillis();
      while (timeoutMS >= 0) {
        try {
          mThreadPool.awaitTermination(timeoutMS, TimeUnit.MILLISECONDS);
          break;
        } catch (InterruptedException e) {
          long newnow = System.currentTimeMillis();
          timeoutMS -= (newnow - now);
          now = newnow;
        }
      }
    }
  }
}
