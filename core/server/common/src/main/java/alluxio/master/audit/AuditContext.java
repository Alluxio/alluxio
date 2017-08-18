package alluxio.master.audit;

import java.io.Closeable;

public interface AuditContext extends Closeable {
  void append();

  AuditContext setAllowed(boolean allowed);

  void setCommitted(boolean committed);

  boolean isCommitted();

  @Override
  void close();
}
