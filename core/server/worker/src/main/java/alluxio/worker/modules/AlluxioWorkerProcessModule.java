package alluxio.worker.modules;

import alluxio.conf.Configuration;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.AlluxioWorkerProcess;
import alluxio.worker.WorkerFactory;
import alluxio.worker.WorkerProcess;
import alluxio.worker.dora.DoraWorker;
import alluxio.worker.dora.DoraWorkerFactory;
import alluxio.worker.dora.PagedDoraWorker;
import java.net.InetSocketAddress;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

/**
 * Grpc worker module.
 */
public class AlluxioWorkerProcessModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(WorkerFactory.class).to(DoraWorkerFactory.class).in(Scopes.SINGLETON);
    bind(WorkerProcess.class).to(AlluxioWorkerProcess.class).in(Scopes.SINGLETON);
  }
}
