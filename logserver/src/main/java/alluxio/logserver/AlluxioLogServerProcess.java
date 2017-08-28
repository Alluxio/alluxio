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
import alluxio.Process;
import alluxio.PropertyKey;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
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
import java.util.Properties;

/**
 * A centralized log server for Alluxio
 *
 * Alluxio masters and workers generate logs and store the logs in local storage.
 * {@link AlluxioLogServerProcess} allows masters and workers to "push" their logs to a
 * centralized log server where another copy of the logs will be stored.
 */
public class AlluxioLogServerProcess implements Process {
  private static final String LOGSERVER_CLIENT_LOGGER_APPENDER_NAME = "LOGSERVER_CLIENT_LOGGER";
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioLogServerProcess.class);
  private static final long STOP_TIMEOUT_MS = 60000;
  private static final int BASE_SLEEP_TIME_MS = 50;
  private static final int MAX_SLEEP_TIME_MS = 30000;
  private static final int MAX_NUM_RETRY = 20;

  private final String mBaseLogsDir;
  private int mPort;
  private ServerSocket mServerSocket;
  private final int mMinNumberOfThreads;
  private final int mMaxNumberOfThreads;
  private ExecutorService mThreadPool;
  private volatile boolean mStopped;

  /**
   * Construct an {@link AlluxioLogServerProcess} instance.
   *
   * @param baseLogsDir base directory to store the logs pushed from remote Alluxio servers
   */
  public AlluxioLogServerProcess(String baseLogsDir) {
    mPort = Configuration.getInt(PropertyKey.LOG_SERVER_PORT);
    // The log server serves the logging requests from Alluxio workers, Alluxio master, Alluxio
    // secondary master, and Alluxio proxy. Therefore the number of threads required by
    // log server is #workers + 1 (master) + 1 (secondary master) + 1 (proxy).
    mMinNumberOfThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MIN) + 3;
    mMaxNumberOfThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MAX) + 3;
    mBaseLogsDir = baseLogsDir;
    mStopped = true;
  }

  @Override
  public void start() throws Exception {
    startServing();
  }

  @Override
  public void stop() throws Exception {
    stopServing();
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
   * Create and start logging server and client thread pool.
   */
  private void startServing() {
    SynchronousQueue<Runnable> synchronousQueue =
        new SynchronousQueue<>();
    mThreadPool =
        new ThreadPoolExecutor(mMinNumberOfThreads, mMaxNumberOfThreads,
            STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS, synchronousQueue);
    try {
      mServerSocket = new ServerSocket(mPort);
    } catch (IOException e) {
      LOG.error("Failed to bind to port {}.", mPort);
      throw new RuntimeException(e);
    }
    mStopped = false;
    while (!mStopped) {
      Socket client;
      try {
        client = mServerSocket.accept();
      } catch (IOException e) {
        LOG.warn("I/O error occured while waiting for connection.");
        throw new RuntimeException(e);
      }
      InetAddress inetAddress = client.getInetAddress();
      AlluxioLog4jSocketNode clientSocketNode = new AlluxioLog4jSocketNode(this, client);
      RetryPolicy retryPolicy = new ExponentialBackoffRetry(
          BASE_SLEEP_TIME_MS, MAX_SLEEP_TIME_MS, MAX_NUM_RETRY);
      while (true) {
        try {
          mThreadPool.execute(clientSocketNode);
          break;
        } catch (RejectedExecutionException e) {
          if (!retryPolicy.attemptRetry()) {
            LOG.warn("Connection with {} has been rejected by ExecutorService {} times"
                    + "till timedout, reason: {}",
                inetAddress.getHostAddress(), retryPolicy.getRetryCount(), e);
            // Alluxio log clients (master, secondary master, proxy and workers establish
            // long-living connections with the log server. Therefore, if the log server cannot
            // find a thread to service a log client, it is very likely due to low number of
            // worker threads. If retry fails, then it makes sense just to let system throw
            // an exception. The system admin should increase the thread pool size.
            throw new RuntimeException(
                "Increase the number of worker threads in the thread pool", e);
          }
        } catch (Error | Exception e) {
          LOG.error("ExecutorService threw error: ", e);
          throw e;
        }
      }
    }
  }

  /**
   * Stop the main thread of {@link AlluxioLogServerProcess}.
   *
   * Close the server socket, shutdown the thread pool, stop accepting new requests,
   * and blocks until all worker threads terminate.
   */
  private void stopServing() {
    mStopped = true;
    if (mServerSocket != null) {
      try {
        mServerSocket.close();
      } catch (IOException e) {
        LOG.warn("Exception in closing server socket.", e);
      }
    }

    mThreadPool.shutdown();
    long timeoutMS = STOP_TIMEOUT_MS;
    long now = System.currentTimeMillis();
    while (timeoutMS >= 0) {
      try {
        boolean ret = mThreadPool.awaitTermination(timeoutMS, TimeUnit.MILLISECONDS);
        if (ret) {
          LOG.info("All worker threads have terminated.");
        } else {
          LOG.warn("Log server has timeout waiting for worker threads to terminate.");
        }
        break;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        long newnow = System.currentTimeMillis();
        timeoutMS -= (newnow - now);
        now = newnow;
      }
    }
  }

  /**
   * Configure a {@link Hierarchy} instance used to retrive logger by name and maintain the logger
   * hierarchy. An instance of this class will be passed to a {@link AlluxioLog4jSocketNode} so
   * that the {@link AlluxioLog4jSocketNode} instance can retrieve the logger to log incoming
   * {@link org.apache.log4j.spi.LoggingEvent}s.
   *
   * @param inetAddress inet address of the client
   * @param logAppenderName name of the appender to use for this client
   * @return a {@link Hierarchy} instance to pass to {@link AlluxioLog4jSocketNode}
   * @throws IOException if fails to create an {@link FileInputStream} to read log4j.properties
   */
  protected LoggerRepository configureHierarchy(InetAddress inetAddress, String logAppenderName)
      throws IOException {
    Hierarchy clientHierarchy;
    String inetAddressStr = inetAddress.getHostAddress();
    Properties properties = new Properties();
    File configFile;
    try {
      configFile = new File(new URI(System.getProperty("log4j.configuration")));
    } catch (URISyntaxException e) {
      // Alluxio log server cannot derive a valid path to log4j.properties. Since this
      // properties file is global, we should throw an exception.
      LOG.error("Cannot derive a valid URI to log4j.properties file.");
      throw new RuntimeException(e);
    }
    try (FileInputStream inputStream = new FileInputStream(configFile)) {
      properties.load(inputStream);
    }
    Level level = Level.INFO;
    clientHierarchy = new Hierarchy(new RootLogger(level));
    // Startup script should guarantee that mBaseLogsDir already exists.
    String logDirectoryPath = mBaseLogsDir + "/" + logAppenderName.toLowerCase();
    File logDirectory = new File(logDirectoryPath);
    if (!logDirectory.exists()) {
      logDirectory.mkdir();
    }
    String logFilePath = logDirectoryPath + "/" + inetAddressStr + ".log";
    properties.setProperty("log4j.rootLogger",
        level.toString() + "," + LOGSERVER_CLIENT_LOGGER_APPENDER_NAME);
    properties.setProperty("log4j.appender." + LOGSERVER_CLIENT_LOGGER_APPENDER_NAME + ".File",
        logFilePath);
    new PropertyConfigurator().doConfigure(properties, clientHierarchy);
    return clientHierarchy;
  }
}
