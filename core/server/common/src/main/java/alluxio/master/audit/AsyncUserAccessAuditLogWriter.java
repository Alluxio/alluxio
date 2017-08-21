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

import alluxio.Configuration;
import alluxio.PropertyKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;

/**
 * {@link AsyncUserAccessAuditLogWriter} writes user access audit log entries asynchronously.
 */
@ThreadSafe
public final class AsyncUserAccessAuditLogWriter {
  private static final int QUEUE_SIZE = 10000;
  private static final Logger LOG = LoggerFactory.getLogger(AsyncUserAccessAuditLogWriter.class);
  private final boolean mEnabled;
  private volatile boolean mStopped;
  private LinkedBlockingQueue<AuditContext> mAuditLogEntries;

  /**
   * Constructs an {@link AsyncUserAccessAuditLogWriter} instance.
   */
  public AsyncUserAccessAuditLogWriter() {
    mEnabled = Boolean.parseBoolean(Configuration.get(PropertyKey.MASTER_AUDIT_LOGGING_ENABLED));
    if (mEnabled) {
      mAuditLogEntries = new LinkedBlockingQueue<>(QUEUE_SIZE);
    }
    mStopped = true;
  }

  /**
   * Checks whether user access audit logging is enabled or not.
   *
   * @return true if user access audit is enabled, false otherwise
   */
  public boolean isEnabled() {
    return mEnabled;
  }

  /**
   * Starts {@link AsyncUserAccessAuditLogWriter}.
   */
  public void start() {
    if (mEnabled && mStopped) {
      mStopped = false;
      new Thread(new AuditLoggingWorker()).start();
    }
  }

  /**
   * Stops {@link AsyncUserAccessAuditLogWriter}.
   */
  public void stop() {
    mStopped = true;
  }

  /**
   * Appends an audit context.
   *
   * @param context the audit context to append
   * @return true if append operation succeeds, false otherwise
   */
  public boolean append(AuditContext context) {
    try {
      mAuditLogEntries.put(context);
    } catch (InterruptedException e) {
      // Reset the interrupted flag and return because some other thread has
      // told us not to wait any more.
      Thread.currentThread().interrupt();
      return false;
    }
    return true;
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
          LOG.info(headContext.toString());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }
}
