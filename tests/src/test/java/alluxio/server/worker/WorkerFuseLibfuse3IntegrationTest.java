package alluxio.server.worker;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

public class WorkerFuseLibfuse3IntegrationTest extends WorkerFuseIntegrationTest {
  @Override
  public void configure() {
    super.configure();
    ServerConfiguration.set(PropertyKey.FUSE_JNIFUSE_LIBFUSE_VERSION, 3);
  }
}
