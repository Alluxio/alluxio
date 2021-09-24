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

package alluxio.master.audit;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import java.util.concurrent.LinkedBlockingQueue;
import javax.annotation.concurrent.ThreadSafe;

/**
 * {@link AsyncUserAccessAuditLogWriter} writes user access audit log entries asynchronously.
 *
 * TODO(yanqin) investigate other lock-free queues, e.g. Lmax disruptor used by Lmax and log4j 2
 */
@ThreadSafe
public final class AsyncUserAccessAuditLogWriter {
  private static final String AUDIT_LOG_THREAD_NAME = "AsyncUserAccessAuditLogger";
  private static final Logger LOG =
      LoggerFactory.getLogger(AsyncUserAccessAuditLogWriter.class);
  private static final Logger AUDIT_LOG =
      LoggerFactory.getLogger("AUDIT_LOG");
  private volatile boolean mStopped;
  /**
   * A thread-safe linked-list-based queue with an optional capacity limit.
   */
  private LinkedBlockingQueue<AuditContext> mAuditLogEntries;

  /**
   * Background thread that performs actual log writing.
   */
  private Thread mLoggingWorkerThread;

  /**
   * Constructs an {@link AsyncUserAccessAuditLogWriter} instance.
   */
  public AsyncUserAccessAuditLogWriter() {
    int queueCapacity = ServerConfiguration.getInt(PropertyKey.MASTER_AUDIT_LOGGING_QUEUE_CAPACITY);
    mAuditLogEntries = new LinkedBlockingQueue<>(queueCapacity);
    LOG.info("Audit logging queue capacity is {}.", queueCapacity);
    mStopped = true;
  }

  /**
   * Starts {@link AsyncUserAccessAuditLogWriter}.
   */
  public synchronized void start() {
    if (mStopped) {
      Preconditions.checkState(mLoggingWorkerThread == null);
      mStopped = false;
      mLoggingWorkerThread = new Thread(new AuditLoggingWorker());
      mLoggingWorkerThread.setName(AUDIT_LOG_THREAD_NAME);
      mLoggingWorkerThread.start();
      LOG.info("AsyncUserAccessAuditLogWriter thread started.");
    }
  }

  /**
   * Stops {@link AsyncUserAccessAuditLogWriter}.
   */
  public synchronized void stop() {
    if (!mStopped) {
      mLoggingWorkerThread.interrupt();
      try {
        mLoggingWorkerThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        mStopped = true;
        mLoggingWorkerThread = null;
        LOG.info("AsyncUserAccessAuditLogWriter thread stopped.");
      }
    }
  }

  /**
   * Appends an {@link AuditContext}.
   *
   * @param context the audit context to append
   * @return true if append operation succeeds, false otherwise
   */
  public boolean append(AuditContext context) {
    try {
      mAuditLogEntries.put(context);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
    return true;
  }

  /**
   * Gets the size of audit log entries.
   * @return the size of the audit log blocking queue
   */
  public long getAuditLogEntriesSize() {
    return mAuditLogEntries.size();
  }

  /**
   * Consumer thread of the queue to perform actual logging of audit info.
   */
  private class AuditLoggingWorker implements Runnable {
    public AuditLoggingWorker() {}

    @Override
    public void run() {
      while (!mStopped) {
        try {
          AuditContext headContext = mAuditLogEntries.take();
          AUDIT_LOG.info(headContext.toString());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }
}
