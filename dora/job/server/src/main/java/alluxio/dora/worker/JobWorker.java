/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.dora.worker;

import alluxio.dora.ClientContext;
import alluxio.dora.Constants;
import alluxio.dora.Server;
import alluxio.dora.client.file.FileSystem;
import alluxio.dora.client.file.FileSystemContext;
import alluxio.dora.conf.Configuration;
import alluxio.dora.conf.PropertyKey;
import alluxio.dora.exception.ConnectionFailedException;
import alluxio.dora.grpc.GrpcService;
import alluxio.dora.grpc.ServiceType;
import alluxio.dora.heartbeat.HeartbeatContext;
import alluxio.dora.heartbeat.HeartbeatThread;
import alluxio.dora.job.JobServerContext;
import alluxio.dora.metrics.MetricsSystem;
import alluxio.dora.user.ServerUserState;
import alluxio.dora.underfs.UfsManager;
import alluxio.dora.util.executor.ExecutorServiceFactories;
import alluxio.dora.wire.WorkerNetAddress;
import alluxio.dora.worker.job.command.CommandHandlingExecutor;
import alluxio.dora.worker.job.task.TaskExecutorManager;
import alluxio.dora.worker.job.JobMasterClient;
import alluxio.dora.job.JobMasterClientContext;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A job worker that manages all the worker-related activities.
 */
@NotThreadSafe
public final class JobWorker extends AbstractWorker {
  private static final Logger LOG = LoggerFactory.getLogger(JobWorker.class);

  private final JobServerContext mJobServerContext;
  /** Client for job master communication. */
  private final JobMasterClient mJobMasterClient;
  /** The service that handles commands sent from master. */
  private Future<?> mCommandHandlingService;

  /**
   * Creates a new instance of {@link JobWorker}.
   *
   * @param ufsManager the ufs manager
   */
  JobWorker(FileSystem filesystem, FileSystemContext fsContext, UfsManager ufsManager) {
    super(ExecutorServiceFactories.fixedThreadPool("job-worker-executor", 1));
    mJobServerContext = new JobServerContext(filesystem, fsContext, ufsManager);
    mJobMasterClient = JobMasterClient.Factory.create(JobMasterClientContext
        .newBuilder(ClientContext.create(Configuration.global())).build());
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return new HashSet<>();
  }

  @Override
  public String getName() {
    return Constants.JOB_WORKER_NAME;
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    return Collections.emptyMap();
  }

  @Override
  public void start(WorkerNetAddress address) throws IOException {
    super.start(address);

    // Start serving metrics system, this will not block
    MetricsSystem.startSinks(Configuration.getString(PropertyKey.METRICS_CONF_FILE));

    try {
      JobWorkerIdRegistry.registerWorker(mJobMasterClient, address);
    } catch (ConnectionFailedException e) {
      LOG.error("Failed to connect to job master", e);
      throw Throwables.propagate(e);
    }
    TaskExecutorManager taskExecutorManager =
        new TaskExecutorManager(Configuration.getInt(PropertyKey.JOB_WORKER_THREADPOOL_SIZE),
            address);

    mCommandHandlingService = getExecutorService().submit(
        new HeartbeatThread(HeartbeatContext.JOB_WORKER_COMMAND_HANDLING,
            new CommandHandlingExecutor(mJobServerContext, taskExecutorManager, mJobMasterClient,
                address),
            (int) Configuration.getMs(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL),
            Configuration.global(), ServerUserState.global()));
  }

  @Override
  public void stop() throws IOException {
    if (mCommandHandlingService != null) {
      mCommandHandlingService.cancel(true);
    }
    mJobMasterClient.close();

    super.stop();
  }
}
