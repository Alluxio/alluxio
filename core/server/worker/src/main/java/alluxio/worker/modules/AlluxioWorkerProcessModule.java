package alluxio.worker.modules;

import alluxio.worker.AlluxioWorkerProcess;
import alluxio.worker.WorkerFactory;
import alluxio.worker.WorkerProcess;
import alluxio.worker.dora.DoraWorkerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

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
