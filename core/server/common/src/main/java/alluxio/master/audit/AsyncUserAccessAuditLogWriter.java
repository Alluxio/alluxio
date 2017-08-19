package alluxio.master.audit;

import alluxio.Configuration;
import alluxio.PropertyKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class AsyncUserAccessAuditLogWriter {
  private static final int QUEUE_SIZE = 10000;
  private static final int BLOCKING_TIMEOUT_US = 10;
  private static final Logger LOG = LoggerFactory.getLogger(AsyncUserAccessAuditLogWriter.class);
  private boolean mEnabled;
  private volatile boolean mStopped;
  private ArrayBlockingQueue<AuditContext> mAuditLogEntries;

  public AsyncUserAccessAuditLogWriter() {
    mEnabled = Boolean.parseBoolean(Configuration.get(PropertyKey.MASTER_AUDIT_LOGGING_ENABLED));
    if (mEnabled) {
      mAuditLogEntries = new ArrayBlockingQueue<>(QUEUE_SIZE);
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

  public void commit(AuditContext context) {
    synchronized (context) {
      context.setCommitted(true);
      context.notify();
    }
  }

  private class AuditLoggingWorker implements Runnable {
    @Override
    public void run() {
      while (!mStopped) {
        try {
          AuditContext headContext = mAuditLogEntries.poll(BLOCKING_TIMEOUT_US, TimeUnit.MICROSECONDS);
          if (headContext == null) {
            continue;
          }
          synchronized (headContext) {
            while (!headContext.isCommitted()) {
              headContext.wait();
            }
          }
          if (headContext.isCommitted()) {
            LOG.info(headContext.toString());
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
