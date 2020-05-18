package alluxio.worker.file;

import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.conf.ServerConfiguration;
import alluxio.master.MasterClientContext;
import alluxio.resource.DynamicResourcePool;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A pool of {@link FileSystemMasterClient}'s which are used by workers to communicate
 * worker-specific information to the Alluxio master.
 */
public class FileSystemMasterWorkerClientPool extends DynamicResourcePool<FileSystemMasterClient> {

  /**
   * Creates a new instance of {@link FileSystemMasterWorkerClientPool}.
   *
   * @param gcService the executor service used to run GCs
   */
  public FileSystemMasterWorkerClientPool(ScheduledExecutorService gcService) {
    super(DynamicResourcePool.Options.defaultOptions().setGcExecutor(gcService));
  }

  @Override
  protected boolean shouldGc(ResourceInternal<FileSystemMasterClient> resourceInternal) {
    return System.currentTimeMillis() - resourceInternal.getLastAccessTimeMs()
        > Constants.MINUTE_MS;
  }

  @Override
  protected boolean isHealthy(FileSystemMasterClient resource) {
    return !resource.isClosed();
  }

  @Override
  protected void closeResource(FileSystemMasterClient resource) throws IOException {
    resource.close();
  }

  @Override
  protected FileSystemMasterClient createNewResource() throws IOException {
    return new FileSystemMasterClient(
        MasterClientContext.newBuilder(
            ClientContext.create(ServerConfiguration.global())
        ).build());
  }
}
