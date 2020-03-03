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

package alluxio.worker.job;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.grpc.JobCommand;
import alluxio.grpc.JobHeartbeatPRequest;
import alluxio.grpc.JobInfo;
import alluxio.grpc.JobMasterWorkerServiceGrpc;
import alluxio.grpc.RegisterJobWorkerPRequest;
import alluxio.grpc.ServiceType;
import alluxio.grpc.GrpcUtils;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the job service master, used by job service
 * workers.
 */
@ThreadSafe
public final class RetryHandlingJobMasterClient extends AbstractMasterClient
    implements JobMasterClient {
  private JobMasterWorkerServiceGrpc.JobMasterWorkerServiceBlockingStub mClient = null;

  /**
   * Creates a new job master client.
   *
   * @param conf job master client configuration
   */
  public RetryHandlingJobMasterClient(JobMasterClientContext conf) {
    super(conf);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.JOB_MASTER_WORKER_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.JOB_MASTER_WORKER_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.JOB_MASTER_WORKER_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = JobMasterWorkerServiceGrpc.newBlockingStub(mChannel);
  }

  @Override
  public long registerWorker(final WorkerNetAddress address) throws IOException {
    return retryRPC(() -> mClient.registerJobWorker(RegisterJobWorkerPRequest.newBuilder()
            .setWorkerNetAddress(GrpcUtils.toProto(address)).build()).getId());
  }

  @Override
  public List<JobCommand> heartbeat(final JobWorkerHealth jobWorkerHealth,
      final List<JobInfo> taskInfoList) throws IOException {
    return retryRPC(new RpcCallable<List<JobCommand>>() {

      @Override
      public List<JobCommand> call() {
        return mClient.heartbeat(JobHeartbeatPRequest.newBuilder()
            .setJobWorkerHealth(jobWorkerHealth.toProto()).addAllTaskInfos(taskInfoList).build())
            .getCommandsList();
      }
    });
  }
}
