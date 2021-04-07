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

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.grpc.CancelPRequest;
import alluxio.grpc.CancelPResponse;
import alluxio.grpc.GetAllWorkerHealthPRequest;
import alluxio.grpc.GetAllWorkerHealthPResponse;
import alluxio.grpc.GetJobServiceSummaryPRequest;
import alluxio.grpc.GetJobServiceSummaryPResponse;
import alluxio.grpc.GetJobStatusDetailedPRequest;
import alluxio.grpc.GetJobStatusDetailedPResponse;
import alluxio.grpc.GetJobStatusPRequest;
import alluxio.grpc.GetJobStatusPResponse;
import alluxio.grpc.JobMasterClientServiceGrpc;
import alluxio.grpc.ListAllPRequest;
import alluxio.grpc.ListAllPResponse;
import alluxio.grpc.RunPRequest;
import alluxio.grpc.RunPResponse;
import alluxio.grpc.ServiceType;
import alluxio.job.JobConfig;
import alluxio.job.ProtoUtils;
import alluxio.job.util.SerializationUtils;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.JobServiceSummary;
import alluxio.job.wire.JobWorkerHealth;
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
public final class RetryHandlingJobMasterClient extends AbstractMasterClient
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
      CancelPResponse response =
          mClient.cancel(CancelPRequest.newBuilder().setJobId(jobId).build());
      if (RPC_LOG.isDebugEnabled()) {
        RPC_LOG.debug("cancel response has {} bytes", response.getSerializedSize());
      }
      return null;
    }, RPC_LOG, "Cancel", "jobId=%d", jobId);
  }

  @Override
  public JobInfo getJobStatus(final long id) throws IOException {
    return ProtoUtils.fromProto(
        retryRPC(() -> {
          GetJobStatusPResponse response =
              mClient.getJobStatus(GetJobStatusPRequest.newBuilder().setJobId(id).build());
          alluxio.grpc.JobInfo jobInfo = response.getJobInfo();
          if (RPC_LOG.isDebugEnabled()) {
            RPC_LOG.debug("getJobStatus response has {} bytes, {} affected paths, {} children",
                response.getSerializedSize(), jobInfo.getAffectedPathsCount(),
                jobInfo.getChildrenCount());
          }
          return jobInfo;
        }, RPC_LOG, "GetJobStatus", "id=%d", id));
  }

  @Override
  public JobInfo getJobStatusDetailed(long id) throws IOException {
    return ProtoUtils.fromProto(
        retryRPC(() -> {
          GetJobStatusDetailedPResponse response = mClient.getJobStatusDetailed(
              GetJobStatusDetailedPRequest.newBuilder().setJobId(id).build());
          alluxio.grpc.JobInfo jobInfo = response.getJobInfo();
          if (RPC_LOG.isDebugEnabled()) {
            RPC_LOG.debug("getJobStatusDetailed response has {} bytes,"
                    + " {} affected paths, {} children",
                response.getSerializedSize(), jobInfo.getAffectedPathsCount(),
                jobInfo.getChildrenCount());
          }
          return jobInfo;
        }, RPC_LOG, "GetJobStatusDetailed", "id=%d", id));
  }

  @Override
  public JobServiceSummary getJobServiceSummary() throws IOException {
    return new JobServiceSummary(retryRPC(() -> {
      GetJobServiceSummaryPResponse response = mClient
          .getJobServiceSummary(GetJobServiceSummaryPRequest.newBuilder().build());
      alluxio.grpc.JobServiceSummary summary = response.getSummary();
      if (RPC_LOG.isDebugEnabled()) {
        RPC_LOG.debug("getJobServiceSummary response has {} bytes, {} longest running, "
                + "{} recent activities, {} recent failures, {} summary per status",
                response.getSerializedSize(), summary.getLongestRunningCount(),
                summary.getRecentActivitiesCount(), summary.getRecentFailuresCount(),
                summary.getSummaryPerStatusCount());
      }
      return summary;
    }, RPC_LOG, "GetJobServiceSummary", ""));
  }

  @Override
  public List<Long> list() throws IOException {
    return retryRPC(() -> {
      ListAllPResponse response = mClient.listAll(ListAllPRequest.getDefaultInstance());
      if (RPC_LOG.isDebugEnabled()) {
        RPC_LOG.debug("listAll response has {} bytes, {} JobIds, {} JobInfos",
            response.getSerializedSize(), response.getJobIdsCount(), response.getJobInfosCount());
      }
      return response.getJobIdsList();
    }, RPC_LOG, "List", "");
  }

  @Override
  public List<JobInfo> listDetailed() throws IOException {
    List<alluxio.grpc.JobInfo> jobProtoInfos =
        retryRPC(() -> {
          ListAllPResponse response = mClient.listAll(ListAllPRequest.getDefaultInstance());
          if (RPC_LOG.isDebugEnabled()) {
            RPC_LOG.debug("listAll response has {} bytes, {} JobIds, {} JobInfos",
                response.getSerializedSize(), response.getJobIdsCount(),
                response.getJobInfosCount());
          }
          return response.getJobInfosList();
        }, RPC_LOG, "ListDetailed", "");
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
        () -> {
          RunPResponse response =
              mClient.run(RunPRequest.newBuilder().setJobConfig(jobConfigStr).build());
          if (RPC_LOG.isDebugEnabled()) {
            RPC_LOG.debug("run response has {} bytes", response.getSerializedSize());
          }
          return response.getJobId();
        },
        RPC_LOG, "Run", "jobConfig=%s", jobConfig);
  }

  @Override
  public List<JobWorkerHealth> getAllWorkerHealth() throws IOException {
    return retryRPC(() -> {
      GetAllWorkerHealthPResponse response =
          mClient.getAllWorkerHealth(GetAllWorkerHealthPRequest.newBuilder().build());
      if (RPC_LOG.isDebugEnabled()) {
        RPC_LOG.debug("getAllWorkerHealth response has {} bytes, {} WorkerHealth",
            response.getSerializedSize(), response.getWorkerHealthsCount());
      }
      List<alluxio.grpc.JobWorkerHealth> workerHealthsList = response.getWorkerHealthsList();
      return workerHealthsList.stream().map(JobWorkerHealth::new).collect(Collectors.toList());
    }, RPC_LOG, "GetAllWorkerHealth", "");
  }
}
