package alluxio.master.audit;

import java.io.Closeable;

public interface AuditContext extends Closeable {
  void append();

  AuditContext setAllowed(boolean allowed);

  @Override
  void close();
}
