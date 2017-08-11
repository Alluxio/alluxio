package alluxio.master.audit;

import java.util.concurrent.ConcurrentLinkedQueue;

public class UserAccessAuditLog {
  private static ConcurrentLinkedQueue<AuditLogEntry> sAuditLogEntries = new ConcurrentLinkedQueue<AuditLogEntry>();

  public static void append(AuditLogEntry entry) {
    sAuditLogEntries.offer(entry);
  }

  public static AuditLogEntry getNextLogEntry() {
    return sAuditLogEntries.poll();
  }

  public static class AuditLogEntry {
    final String mCommand;
    final String mSrcPath;
    final String mDstPath;
    String mUser;
    String mIp;
    String mSrcPathOwner;
    String mSrcPathGroup;
    Short mSrcPathMode;
    boolean mAuthorized;

    public AuditLogEntry(String cmd, String src, String dst) {
      mCommand = cmd;
      mSrcPath = src;
      mDstPath = dst;
    }
    public String getCommand() { return mCommand; }
    public String getSrcPath() { return mSrcPath; }
    public String getSrcPathOwner() { return mSrcPathOwner; }
    public String getSrcPathGroup() { return mSrcPathGroup; }
    public Short getSrcPathMode() { return mSrcPathMode; }
    public String getDstPath() { return mDstPath; }
    public String getUser() { return mUser; }
    public String getIp() { return mIp; }
    public boolean isAuthorized() { return mAuthorized; }
    public void setSrcPathOwner(String owner) { mSrcPathOwner = owner; }
    public void setSrcPathGroup(String group) { mSrcPathGroup = group; }
    public void setSrcPathMode(short mode) { mSrcPathMode = mode; }
    public void setUser(String user) { mUser = user; }
    public void setIp(String ip) { mIp = ip; }
    public void setAuthorized(boolean authorized) { mAuthorized = authorized; }
  }
}
