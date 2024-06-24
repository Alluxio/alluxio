package alluxio.worker.ucx;

import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.DynamicResourcePool;
import alluxio.util.ThreadFactoryUtils;
import alluxio.worker.ucx.UcxConnection;

import com.codahale.metrics.Counter;
import io.netty.bootstrap.Bootstrap;
import org.openucx.jucx.ucp.UcpWorker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class UcxConnectionPool extends DynamicResourcePool<UcxConnection> {
  private static final Counter COUNTER = MetricsSystem.counter(
      MetricKey.UCX_CONNECTION_COUNT.getName());

  private static final int UCX_CONNECTION_POOL_GC_THREADPOOL_SIZE = 4;
  private static final ScheduledExecutorService GC_EXECUTOR =
      new ScheduledThreadPoolExecutor(UCX_CONNECTION_POOL_GC_THREADPOOL_SIZE,
          ThreadFactoryUtils.build("NettyChannelPoolGcThreads-%d", true));

  private final UcpWorker mWorker;
  private InetSocketAddress mRemoteAddress;

  /**
   * Create a dynamic UcxConnection resource pool.
   * @param remoteAddr
   * @param maxCapacity
   * @param gcThresholdMs
   */
  public UcxConnectionPool(InetSocketAddress remoteAddr, UcpWorker worker,
                           int maxCapacity, long gcThresholdMs) {
    super(Options.defaultOptions().setMaxCapacity(maxCapacity).setGcExecutor(GC_EXECUTOR));
    mRemoteAddress = remoteAddr;
    mWorker = worker;

  }
  @Override
  protected Counter getMetricCounter() {
    return COUNTER;
  }

  @Override
  protected boolean shouldGc(DynamicResourcePool<UcxConnection>.ResourceInternal<UcxConnection>
                                   resourceInternal) {
    return false;
  }

  @Override
  protected boolean isHealthy(UcxConnection resource) {
    return !resource.isClosed();
  }

  @Override
  protected void closeResource(UcxConnection resource) throws IOException {
    resource.close();
  }

  @Override
  protected UcxConnection createNewResource() throws IOException {
    try {
      return UcxConnection.initNewConnection(mRemoteAddress, mWorker);
    } catch (Exception ex) {
      throw new IOException("Error in init new UcxConnection", ex);
    }
  }
}
