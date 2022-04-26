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

package alluxio.client.job;

import alluxio.AbstractJobMasterClient;
import alluxio.Constants;
import alluxio.grpc.CancelPRequest;
import alluxio.grpc.GetAllWorkerHealthPRequest;
import alluxio.grpc.GetCmdStatusDetailedRequest;
import alluxio.grpc.GetCmdStatusRequest;
import alluxio.grpc.GetJobServiceSummaryPRequest;
import alluxio.grpc.GetJobStatusDetailedPRequest;
import alluxio.grpc.GetJobStatusPRequest;
import alluxio.grpc.JobMasterClientServiceGrpc;
import alluxio.grpc.ListAllPOptions;
import alluxio.grpc.ListAllPRequest;
import alluxio.grpc.RunPRequest;
import alluxio.grpc.ServiceType;
import alluxio.grpc.SubmitRequest;
import alluxio.job.CmdConfig;
import alluxio.job.JobConfig;
import alluxio.job.ProtoUtils;
import alluxio.job.util.SerializationUtils;
import alluxio.job.wire.CmdStatusBlock;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.JobServiceSummary;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.job.wire.Status;
import alluxio.worker.job.JobMasterClientContext;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the job service master, used by job service
 * clients.
 */
@ThreadSafe
public final class RetryHandlingJobMasterClient extends AbstractJobMasterClient
    implements JobMasterClient {
  private static final Logger RPC_LOG = LoggerFactory.getLogger(JobMasterClient.class);
  private JobMasterClientServiceGrpc.JobMasterClientServiceBlockingStub mClient = null;

  /**
   * Creates a new job master client.
   *
   * @param conf master client configuration
   */
  public RetryHandlingJobMasterClient(JobMasterClientContext conf) {
    super(conf);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.JOB_MASTER_CLIENT_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.JOB_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.JOB_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = JobMasterClientServiceGrpc.newBlockingStub(mChannel);
  }

  @Override
  public void cancel(final long jobId) throws IOException {
    retryRPC((RpcCallable<Void>) () -> {
      mClient.cancel(CancelPRequest.newBuilder().setJobId(jobId).build());
      return null;
    }, RPC_LOG, "Cancel", "jobId=%d", jobId);
  }

  @Override
  public JobInfo getJobStatus(final long id) throws IOException {
    return ProtoUtils.fromProto(
        retryRPC(() -> mClient.getJobStatus(GetJobStatusPRequest.newBuilder().setJobId(id).build())
            .getJobInfo(), RPC_LOG, "GetJobStatus", "id=%d", id));
  }

  @Override
  public JobInfo getJobStatusDetailed(long id) throws IOException {
    return ProtoUtils.fromProto(
        retryRPC(() -> mClient.getJobStatusDetailed(
            GetJobStatusDetailedPRequest.newBuilder().setJobId(id).build())
            .getJobInfo(), RPC_LOG, "GetJobStatusDetailed", "id=%d", id));
  }

  @Override
  public JobServiceSummary getJobServiceSummary() throws IOException {
    return new JobServiceSummary(retryRPC(() -> mClient
        .getJobServiceSummary(GetJobServiceSummaryPRequest.newBuilder().build()).getSummary(),
        RPC_LOG, "GetJobServiceSummary", ""));
  }

  @Override
  public List<Long> list(ListAllPOptions option) throws IOException {
    return retryRPC(() -> mClient.listAll(
        ListAllPRequest.newBuilder()
            .setOptions(option.toBuilder().setJobIdOnly(true)).build())
            .getJobIdsList(),
        RPC_LOG, "List", "");
  }

  @Override
  public List<JobInfo> listDetailed() throws IOException {
    List<alluxio.grpc.JobInfo> jobProtoInfos =
        retryRPC(() -> mClient.listAll(ListAllPRequest.getDefaultInstance()).getJobInfosList(),
            RPC_LOG, "ListDetailed", "");
    ArrayList<JobInfo> result = Lists.newArrayList();
    for (alluxio.grpc.JobInfo jobProtoInfo : jobProtoInfos) {
      result.add(ProtoUtils.fromProto(jobProtoInfo));
    }
    return result;
  }

  @Override
  public long run(final JobConfig jobConfig) throws IOException {
    final ByteString jobConfigStr = ByteString.copyFrom(SerializationUtils.serialize(jobConfig));
    return retryRPC(
        () -> mClient.run(RunPRequest.newBuilder().setJobConfig(jobConfigStr).build()).getJobId(),
        RPC_LOG, "Run", "jobConfig=%s", jobConfig);
  }

  @Override
  public long submit(CmdConfig cmdConfig) throws IOException {
    final ByteString cmdConfigStr = ByteString.copyFrom(SerializationUtils.serialize(cmdConfig));
    return retryRPC(
        () -> mClient.submit(SubmitRequest.newBuilder().setCmdConfig(cmdConfigStr).build())
            .getJobControlId(), RPC_LOG, "Submit", "cmdConfig=%s", cmdConfig);
  }

  @Override
  public List<JobWorkerHealth> getAllWorkerHealth() throws IOException {
    return retryRPC(() -> {
      List<alluxio.grpc.JobWorkerHealth> workerHealthsList =
          mClient.getAllWorkerHealth(GetAllWorkerHealthPRequest.newBuilder().build())
              .getWorkerHealthsList();
      return workerHealthsList.stream().map(JobWorkerHealth::new).collect(Collectors.toList());
    }, RPC_LOG, "GetAllWorkerHealth", "");
  }

  @Override
  public Status getCmdStatus(long id) throws IOException {
    return ProtoUtils.fromProto(retryRPC(() -> mClient.getCmdStatus(
              GetCmdStatusRequest.newBuilder().setJobControlId(id).build())
              .getCmdStatus(), RPC_LOG, "GetCmdStatus", "jobControlId=%d", id));
  }

  @Override
  public CmdStatusBlock getCmdStatusDetailed(long id) throws IOException {
    return ProtoUtils.protoToCmdStatusBlock(retryRPC(() -> mClient.getCmdStatusDetailed(
                            GetCmdStatusDetailedRequest.newBuilder().setJobControlId(id).build())
                    .getCmdStatusBlock(), RPC_LOG,
            "getCmdStatusDetailed", "jobControlId=%d", id));
  }
}
