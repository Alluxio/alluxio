package alluxio.master.audit;

import alluxio.RpcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

public class UserAccessAuditLog {
  private static final Logger LOG = LoggerFactory.getLogger(UserAccessAuditLog.class);
  private boolean mEnabled;
  private ConcurrentLinkedQueue<RpcUtils.RpcContext> mAuditLogEntries;

  public UserAccessAuditLog() {
    mEnabled = true;
    mAuditLogEntries = new ConcurrentLinkedQueue<>();
  }

  public boolean isEnabled() { return mEnabled; }

  public void log(RpcUtils.RpcContext context) {
    mAuditLogEntries.offer(context);
    RpcUtils.RpcContext head = mAuditLogEntries.poll();
    if (mEnabled && head != null) {
      if (context.isAllowed()) {
        LOG.info("allowed={}\tuser={}\tip={}\tcmd={}\tsrc={}\tdst={}\tperm={}:{}:{}",
            head.isAllowed(), head.getUser(), head.getIp(), head.getCommand(), head.getSrcPath(), head.getDstPath(),
            head.getSrcPathOwner(), head.getSrcPathGroup(), head.getSrcPathMode());
      } else {
        LOG.info("allowed={}\tuser={}\tip={}\tcmd={}\tsrc={}\tdst={}\tperm=null",
            head.isAllowed(), head.getUser(), head.getIp(), head.getCommand(), head.getSrcPath(), head.getDstPath());
      }
    }
  }
}
