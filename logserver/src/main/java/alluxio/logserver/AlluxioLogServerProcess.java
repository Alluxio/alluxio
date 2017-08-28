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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

import java.io.IOException;
import java.net.InetAddress;
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

  /**
   * {@inheritDoc}
   *
   * Create and start logging server and client thread pool.
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
        try {
          ServerSocket tmpServerSocket = new ServerSocket(mPort);
          // If the code reaches here, it indicates mServerSocket has been closed.
          // Therefore, by assigning the reference, we have successfully re-binded
          // mServerSocket.
          mServerSocket = tmpServerSocket;
        } catch (IOException e1) {
          // The original mServerSocket is not closed, therefore just ignore the exception
        }
        continue;
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
   * {@inheritDoc}
   *
   * Stop the main thread of {@link AlluxioLogServerProcess}.
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
    long timeoutMS = THREAD_KEEP_ALIVE_TIME_MS;
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
   * @return the string representation of the path to base logs directory
   */
  public final String getBaseLogsDir() {
    return mBaseLogsDir;
  }
}
