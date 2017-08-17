package alluxio.master.audit;

import alluxio.RpcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;

public class UserAccessAuditLog {
  private static final int QUEUE_SIZE = 100;
  private static final Logger LOG = LoggerFactory.getLogger(UserAccessAuditLog.class);
  private boolean mEnabled;
  private ArrayBlockingQueue<RpcUtils.RpcContext> mAuditLogEntries;

  public UserAccessAuditLog() {
    mEnabled = true;
    mAuditLogEntries = new ArrayBlockingQueue<>(QUEUE_SIZE);
  }

  public boolean isEnabled() { return mEnabled; }

  public boolean append(RpcUtils.RpcContext context) {
    try {
      mAuditLogEntries.put(context);
    } catch (InterruptedException e) {
      // TODO
    }
    return true;
  }

  public void commit(RpcUtils.RpcContext context) {
    synchronized (context) {
      context.setCommitted(true);
      context.notify();
    }

    RpcUtils.RpcContext headContext;
    try {
      headContext = mAuditLogEntries.take();
      synchronized (headContext) {
        while (!headContext.isCommitted()) {
          headContext.wait();
        }
        if (headContext.isAllowed()) {
          LOG.info("allowed={}\tuser={}\tip={}\tcmd={}\tsrc={}\tdst={}\tperm={}:{}:{}",
              headContext.isAllowed(), headContext.getUser(), headContext.getIp(), headContext.getCommand(), headContext.getSrcPath(), headContext.getDstPath(),
              headContext.getSrcPathOwner(), headContext.getSrcPathGroup(), headContext.getSrcPathMode());
        } else {
          LOG.info("allowed={}\tuser={}\tip={}\tcmd={}\tsrc={}\tdst={}\tperm=null",
              headContext.isAllowed(), headContext.getUser(), headContext.getIp(), headContext.getCommand(), headContext.getSrcPath(), headContext.getDstPath());
        }
      }
    } catch (InterruptedException e) {
      // TODO
    }
  }
}
