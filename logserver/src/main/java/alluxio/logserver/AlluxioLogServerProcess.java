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

import alluxio.conf.ServerConfiguration;
import alluxio.Process;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;

/**
 * A centralized log server for Alluxio.
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
  private final int mPort;
  private ServerSocket mServerSocket;
  private final int mMinNumberOfThreads;
  private final int mMaxNumberOfThreads;
  @GuardedBy("mClientSockets")
  private final Set<Socket> mClientSockets = new HashSet<>();
  private ExecutorService mThreadPool;
  private volatile boolean mStopped;

  /**
   * Constructs an {@link AlluxioLogServerProcess} instance.
   *
   * @param baseLogsDir base directory to store the logs pushed from remote Alluxio servers
   */
  public AlluxioLogServerProcess(String baseLogsDir) {
    mPort = ServerConfiguration.getInt(PropertyKey.LOGSERVER_PORT);
    // The log server serves the logging requests from Alluxio servers.
    mMinNumberOfThreads = ServerConfiguration.getInt(PropertyKey.LOGSERVER_THREADS_MIN);
    mMaxNumberOfThreads = ServerConfiguration.getInt(PropertyKey.LOGSERVER_THREADS_MAX);
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
      LOG.info("Starting thread to read logs from {}", clientAddress);
      AlluxioLog4jSocketNode clientSocketNode = new AlluxioLog4jSocketNode(mBaseLogsDir, client);
      synchronized (mClientSockets) {
        mClientSockets.add(client);
      }
      try {
        CompletableFuture.runAsync(clientSocketNode, mThreadPool)
            .whenComplete((r, e) -> {
              synchronized (mClientSockets) {
                mClientSockets.remove(client);
              }
            });
      } catch (RejectedExecutionException e) {
        // Alluxio log clients (master, secondary master, proxy and workers) establish
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
        LOG.warn("Exception in closing server socket: {}", e.toString());
      }
    }
    mThreadPool.shutdownNow();
    // Close all client sockets so that their serving threads can stop. We shut down the threadpool
    // before closing the sockets so that no new socket threads will be created after we try to
    // close them all.
    synchronized (mClientSockets) {
      for (Socket socket : mClientSockets) {
        try {
          socket.close();
        } catch (IOException e) {
          LOG.warn("Exception in closing client socket: {}", e.toString());
        }
      }
    }
    boolean ret = mThreadPool.awaitTermination(THREAD_KEEP_ALIVE_TIME_MS, TimeUnit.MILLISECONDS);
    if (ret) {
      LOG.info("All worker threads have terminated.");
    } else {
      LOG.warn("Log server has timed out waiting for worker threads to terminate.");
    }
  }

  @Override
  public boolean waitForReady(int timeoutMs) {
    try {
      CommonUtils.waitFor(this + " to start", () -> mStopped == false,
          WaitForOptions.defaults().setTimeoutMs(timeoutMs));
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException e) {
      return false;
    }
  }
}
