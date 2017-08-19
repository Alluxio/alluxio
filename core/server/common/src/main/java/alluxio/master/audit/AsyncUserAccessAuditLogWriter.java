package alluxio.master.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class AsyncUserAccessAuditLogWriter {
  private static final int QUEUE_SIZE = 100;
  private static final Logger LOG = LoggerFactory.getLogger(AsyncUserAccessAuditLogWriter.class);
  private boolean mEnabled;
  private ArrayBlockingQueue<AuditContext> mAuditLogEntries;

  public AsyncUserAccessAuditLogWriter() {
    mEnabled = true;
    mAuditLogEntries = new ArrayBlockingQueue<>(QUEUE_SIZE);
  }

  public boolean isEnabled() { return mEnabled; }

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

    AuditContext headContext = mAuditLogEntries.poll();
    if (headContext == null) { return; }
    synchronized (headContext) {
      while (!headContext.isCommitted()) {
        try {
          headContext.wait();
        } catch (InterruptedException e) {
          // TODO
          return;
        }
      }
    }
    LOG.info(headContext.toString());
  }
}
