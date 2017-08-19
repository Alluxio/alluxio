package alluxio.master.audit;

import java.io.Closeable;

/**
 * Context for Alluxio audit logging.
 */
public interface AuditContext extends Closeable {
  void append();

  AuditContext setAllowed(boolean allowed);

  @Override
  void close();
}
