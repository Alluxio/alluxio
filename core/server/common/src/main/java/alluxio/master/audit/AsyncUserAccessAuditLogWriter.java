package alluxio.master.audit;

import alluxio.Configuration;
import alluxio.PropertyKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class AsyncUserAccessAuditLogWriter {
  private static final int QUEUE_SIZE = 10000;
  private static final int BLOCKING_TIMEOUT_US = 10;
  private static final Logger LOG = LoggerFactory.getLogger(AsyncUserAccessAuditLogWriter.class);
  private final boolean mEnabled;
  private volatile boolean mStopped;
  private LinkedBlockingQueue<AuditContext> mAuditLogEntries;

  public AsyncUserAccessAuditLogWriter() {
    mEnabled = Boolean.parseBoolean(Configuration.get(PropertyKey.MASTER_AUDIT_LOGGING_ENABLED));
    if (mEnabled) {
      mAuditLogEntries = new LinkedBlockingQueue<>(QUEUE_SIZE);
    }
    mStopped = true;
  }

  public boolean isEnabled() { return mEnabled; }

  public void start() {
    if (mEnabled && mStopped) {
      mStopped = false;
      new Thread(new AuditLoggingWorker()).start();
    }
  }

  public void stop() { mStopped = true; }

  /**
   * Appends an audit context
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
   * Consumer thread of the queue to perform actual logging of audit info
   */
  private class AuditLoggingWorker implements Runnable {
    @Override
    public void run() {
      while (!mStopped) {
        try {
          AuditContext headContext = mAuditLogEntries.poll(BLOCKING_TIMEOUT_US, TimeUnit.MICROSECONDS);
          if (headContext == null) {
            continue;
          }
          LOG.info(headContext.toString());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }
}
