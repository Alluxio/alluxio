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

package alluxio.worker;

import alluxio.ClientContext;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.ServerConfiguration;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.Server;
import alluxio.exception.ConnectionFailedException;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.job.JobServerContext;
import alluxio.metrics.MetricsSystem;
import alluxio.security.user.ServerUserState;
import alluxio.underfs.UfsManager;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.job.JobMasterClient;
import alluxio.worker.job.JobMasterClientContext;
import alluxio.worker.job.command.CommandHandlingExecutor;
import alluxio.worker.job.task.TaskExecutorManager;

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
  /** The manager for the all the local task execution. */
  private TaskExecutorManager mTaskExecutorManager;
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
        .newBuilder(ClientContext.create(ServerConfiguration.global())).build());
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
    MetricsSystem.startSinks(ServerConfiguration.get(PropertyKey.METRICS_CONF_FILE));

    try {
      JobWorkerIdRegistry.registerWorker(mJobMasterClient, address);
    } catch (ConnectionFailedException e) {
      LOG.error("Failed to connect to job master", e);
      throw Throwables.propagate(e);
    }

    mTaskExecutorManager = new TaskExecutorManager(
        ServerConfiguration.getInt(PropertyKey.JOB_WORKER_THREADPOOL_SIZE), address);

    mCommandHandlingService = getExecutorService().submit(
        new HeartbeatThread(HeartbeatContext.JOB_WORKER_COMMAND_HANDLING,
            new CommandHandlingExecutor(mJobServerContext, mTaskExecutorManager, mJobMasterClient,
                address),
            (int) ServerConfiguration.getMs(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL),
            ServerConfiguration.global(), ServerUserState.global()));
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
