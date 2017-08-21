package alluxio.master.audit;

import java.io.Closeable;

/**
 * Context for Alluxio audit logging.
 */
public interface AuditContext extends Closeable {

  /**
   * Set to true if the operation associated with this {@link AuditContext} is allowed, false otherwise
   *
   * @param allowed true if operation is allowed, false otherwise
   * @return {@link AuditContext} instance itself
   */
  AuditContext setAllowed(boolean allowed);

  /**
   * Operation to perform when this {@link AuditContext} instance is closed
   */
  @Override
  void close();
}
