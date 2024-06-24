package alluxio.worker.ucx;

import alluxio.client.file.cache.CacheManager;
import alluxio.conf.Configuration;
import alluxio.proto.client.Cache;
import alluxio.worker.dora.PagedDoraWorker;
import alluxio.worker.netty.NettyDataServer;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import java.io.IOException;
import java.net.InetSocketAddress;
import javax.inject.Named;

public class UcpServerModule extends AbstractModule {

  private final boolean mUcpServerEnable;

  /**
   * The constructor of UcpServerModule.
   * @param isUcpEnable
   */
  public UcpServerModule(boolean isUcpEnable) {
    mUcpServerEnable = isUcpEnable;
  }

  @Override
  protected void configure() {
    if (!mUcpServerEnable) {
      bind(UcpServer.class).toProvider(() -> null);
    }
  }

}
