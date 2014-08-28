package tachyon;

/**
 * The interface for a periodical heartbeat. <code>HeartbeatThread</code> calls the
 * <code>heartbeat()</code> method.
 */
public interface HeartbeatExecutor {
  public void heartbeat();
}
