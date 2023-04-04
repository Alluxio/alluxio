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

package alluxio.dora.worker.job;

import alluxio.dora.AbstractJobMasterClient;
import alluxio.dora.Constants;
import alluxio.dora.grpc.GrpcUtils;
import alluxio.dora.grpc.JobCommand;
import alluxio.dora.grpc.JobHeartbeatPRequest;
import alluxio.dora.grpc.JobInfo;
import alluxio.dora.grpc.JobMasterWorkerServiceGrpc;
import alluxio.dora.grpc.RegisterJobWorkerPRequest;
import alluxio.dora.grpc.ServiceType;
import alluxio.dora.job.JobMasterClientContext;
import alluxio.dora.job.wire.JobWorkerHealth;
import alluxio.dora.util.CommonUtils;
import alluxio.dora.wire.WorkerNetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the job service master, used by job service
 * workers.
 */
@ThreadSafe
public final class RetryHandlingJobMasterClient extends AbstractJobMasterClient
    implements JobMasterClient {
  private static final Logger RPC_LOG = LoggerFactory.getLogger(JobMasterClient.class);
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
            .setWorkerNetAddress(GrpcUtils.toProto(address)).build()).getId(),
        RPC_LOG, "RegisterWorker", "address=%s", address);
  }

  @Override
  public List<JobCommand> heartbeat(final JobWorkerHealth jobWorkerHealth,
      final List<JobInfo> taskInfoList) throws IOException {
    return retryRPC(() ->
        mClient.heartbeat(JobHeartbeatPRequest.newBuilder()
            .setJobWorkerHealth(jobWorkerHealth.toProto()).addAllTaskInfos(taskInfoList).build())
            .getCommandsList(),
        RPC_LOG, "Heartbeat", "jobWorkerHealth=%s,taskInfoList=%s", jobWorkerHealth,
            CommonUtils.summarizeCollection(taskInfoList));
  }
}
