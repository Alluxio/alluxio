package alluxio.client.fuse;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

public class JNIFuseLibfuse3IntegrationTest extends JNIFuseIntegrationTest {
  @Override
  public void configure() {
    super.configure();
    ServerConfiguration.set(PropertyKey.FUSE_JNIFUSE_LIBFUSE_VERSION, 3);
  }
}
