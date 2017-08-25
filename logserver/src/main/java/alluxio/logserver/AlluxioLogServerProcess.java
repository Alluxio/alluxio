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
  private static final long STOP_TIMEOUT_IN_MS = 60000;
  private static final long REQUEST_TIMEOUT_IN_MS = 20000;
  private static final int BACKOFF_SLOT_IN_MS = 100;

  private final String mBaseLogsDir;
  private int mPort;
  private ServerSocket mServerSocket;
  private final int mMinNumberOfThreads;
  private final int mMaxNumberOfThreads;
  private ExecutorService mThreadPool;
  private volatile boolean mStopped;

  private final Random mRandom = new Random(System.currentTimeMillis());

  /**
   * Construct an {@link AlluxioLogServerProcess} instance.
   *
   * @param baseLogsDir base directory to store the logs pushed from remote Alluxio servers
   */
  public AlluxioLogServerProcess(String baseLogsDir) {
    mPort = Configuration.getInt(PropertyKey.LOG_SERVER_PORT);
    mMinNumberOfThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MIN) + 2;
    mMaxNumberOfThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MAX) + 2;
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
            STOP_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS, synchronousQueue);
    try {
      mServerSocket = new ServerSocket(mPort);
    } catch (IOException e) {
      LOG.error("Failed to bind to port {}.", mPort);
      return;
    }
    int failureCount = 0;
    mStopped = false;
    while (!mStopped) {
      try {
        Socket client = mServerSocket.accept();
        InetAddress inetAddress = client.getInetAddress();
        AlluxioLog4jSocketNode clientSocketNode =
            new AlluxioLog4jSocketNode(this, client);
        int retryCount = 0;
        long remainTimeInMillis = REQUEST_TIMEOUT_IN_MS;
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
                      * (1L << Math.min(retryCount, 20)))) * BACKOFF_SLOT_IN_MS;
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
            } else if (t instanceof Error) {
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
  }

  /**
   * Called from another thread (actually in the shutdown hook of {@link AlluxioLogServer}
   * to stop the main thread of {@link AlluxioLogServerProcess}.
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
    long timeoutMS = STOP_TIMEOUT_IN_MS;
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

  /**
   * Configure a {@link Hierarchy} instance used to retrive logger by name and maintain the logger
   * hierarchy. An instance of this class will be passed to a {@link AlluxioLog4jSocketNode} so
   * that the {@link AlluxioLog4jSocketNode} instance can retrieve the logger to log incoming
   * {@link org.apache.log4j.spi.LoggingEvent}s.
   *
   * @param inetAddress inet address of the client
   * @param logAppenderName name of the appender to use for this client
   * @return A {@link Hierarchy} instance to pass to {@link AlluxioLog4jSocketNode}
   * @throws IOException if fails to create an {@link FileInputStream} to read log4j.properties
   * @throws URISyntaxException if fails to derive a valid URI from the value of property named
   *         "log4j.configuration"
   */
  protected LoggerRepository configureHierarchy(InetAddress inetAddress, String logAppenderName)
      throws IOException {
    Hierarchy clientHierarchy;
    String inetAddressStr = inetAddress.getHostAddress();
    Properties properties = new Properties();
    try {
      final File configFile = new File(new URI(System.getProperty("log4j.configuration")));
      try (FileInputStream inputStream = new FileInputStream(configFile)) {
        properties.load(inputStream);
      }
    } catch (URISyntaxException e) {
      // Alluxio log server cannot derive a valid path to log4j.properties. Since this
      // properties file is global, we should throw an exception.
      throw new RuntimeException(e);
    }
    Level level = Level.INFO;
    clientHierarchy = new Hierarchy(new RootLogger(level));
    String logFilePath = mBaseLogsDir;
    if (logAppenderName.contains("MASTER")) {
      logFilePath += ("/master_logs/" + inetAddressStr + ".master.log");
    } else if (logAppenderName.contains("WORKER")) {
      logFilePath += ("/worker_logs/" + inetAddressStr + ".worker.log");
    } else {
      // Should not reach here
      throw new IllegalStateException("Unknown logger type");
    }
    properties.setProperty("log4j.rootLogger", level.toString() + "," + logAppenderName);
    properties.setProperty("log4j.appender." + logAppenderName + ".File", logFilePath);
    new PropertyConfigurator().doConfigure(properties, clientHierarchy);
    return clientHierarchy;
  }
}
