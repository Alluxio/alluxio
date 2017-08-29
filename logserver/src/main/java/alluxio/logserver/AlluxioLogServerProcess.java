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
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * A centralized log server for Alluxio
 *
 * Alluxio masters and workers generate logs and store the logs in local storage.
 * {@link AlluxioLogServerProcess} allows masters and workers to "push" their logs to a
 * centralized log server where another copy of the logs will be stored.
 */
public class AlluxioLogServerProcess implements Process {
  /**
   * Name of the appender used by log server to perform actual logging of messages received from
   * remote Alluxio servers. It has to match the value in log4j.properties.
   * */
  public static final String LOGSERVER_CLIENT_LOGGER_APPENDER_NAME = "LOGSERVER_CLIENT_LOGGER";
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioLogServerProcess.class);
  private static final long THREAD_KEEP_ALIVE_TIME_MS = 60000;

  private final String mBaseLogsDir;
  private int mPort;
  private ServerSocket mServerSocket;
  private final int mMinNumberOfThreads;
  private final int mMaxNumberOfThreads;
  private ExecutorService mThreadPool;
  private volatile boolean mStopped;

  /**
   * Constructs an {@link AlluxioLogServerProcess} instance.
   *
   * @param baseLogsDir base directory to store the logs pushed from remote Alluxio servers
   */
  public AlluxioLogServerProcess(String baseLogsDir) {
    mPort = Configuration.getInt(PropertyKey.LOGSERVER_PORT);
    // The log server serves the logging requests from Alluxio servers.
    mMinNumberOfThreads = Configuration.getInt(PropertyKey.LOGSERVER_THREADS_MIN);
    mMaxNumberOfThreads = Configuration.getInt(PropertyKey.LOGSERVER_THREADS_MAX);
    mBaseLogsDir = baseLogsDir;
    mStopped = true;
  }

  /**
   * {@inheritDoc}
   *
   * Creates and starts logging server and client thread pool.
   */
  @Override
  public void start() throws Exception {
    SynchronousQueue<Runnable> synchronousQueue = new SynchronousQueue<>();
    mThreadPool =
        new ThreadPoolExecutor(mMinNumberOfThreads, mMaxNumberOfThreads,
            THREAD_KEEP_ALIVE_TIME_MS, TimeUnit.MILLISECONDS, synchronousQueue);
    mStopped = false;
    try {
      mServerSocket = new ServerSocket(mPort);
    } catch (IOException e) {
      throw new RuntimeException("Failed to bind to port {}.", e);
    }
    while (!mStopped) {
      Socket client;
      try {
        client = mServerSocket.accept();
      } catch (IOException e) {
        if (mServerSocket.isClosed()) {
          break;
        } else {
          continue;
        }
      }
      String clientAddress = client.getInetAddress().getHostAddress();
      AlluxioLog4jSocketNode clientSocketNode = new AlluxioLog4jSocketNode(mBaseLogsDir, client);
      try {
        mThreadPool.execute(clientSocketNode);
      } catch (RejectedExecutionException e) {
        // Alluxio log clients (master, secondary master, proxy and workers establish
        // long-living connections with the log server. Therefore, if the log server cannot
        // find a thread to service a log client, it is very likely due to low number of
        // worker threads. If retry fails, then it makes sense just to let system throw
        // an exception. The system admin should increase the thread pool size.
        String errorMessage = String.format(
            "Log server cannot find a worker thread to service log requests from %s. Increase"
                + " the number of worker threads in the thread pool by configuring"
                + " alluxio.logserver.threads.max in alluxio-site.properties. Current value"
                + " is %d.", clientAddress, mMaxNumberOfThreads);
        throw new RuntimeException(errorMessage, e);
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * Stops the main thread of {@link AlluxioLogServerProcess}.
   *
   * Close the server socket, shutdown the thread pool, stop accepting new requests,
   * and blocks until all worker threads terminate.
   */
  @Override
  public void stop() throws Exception {
    mStopped = true;
    if (mServerSocket != null) {
      try {
        mServerSocket.close();
      } catch (IOException e) {
        LOG.warn("Exception in closing server socket.", e);
      }
    }

    mThreadPool.shutdown();
    boolean ret = mThreadPool.awaitTermination(THREAD_KEEP_ALIVE_TIME_MS, TimeUnit.MILLISECONDS);
    if (ret) {
      LOG.info("All worker threads have terminated.");
    } else {
      LOG.warn("Log server has timed out waiting for worker threads to terminate.");
    }
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
}
